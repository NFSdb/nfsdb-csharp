#region copyright
/*
 * Copyright (c) 2014. APAF http://apafltd.co.uk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Server;
using Apaf.NFSdb.Core.Tx;
using log4net;

namespace Apaf.NFSdb.Core.Storage
{
    public class PartitionManager<T> : IPartitionManager<T>
    {
        // ReSharper disable once StaticFieldInGenericType
        private static readonly ILog LOG = LogManager.GetLogger(typeof (PartitionManagerUtils));
        private const int SYMBOL_PARTITION_ID = MetadataConstants.SYMBOL_PARTITION_ID;
        private readonly ICompositeFileFactory _fileFactory;
        private readonly IJournalServer _server;
        private readonly IJournalMetadata<T> _metadata;
        private readonly List<IPartition<T>> _partitions = new List<IPartition<T>>();
        private readonly JournalSettings _settings;
        private readonly object _lastTransLogSync = new object();
        private readonly ColumnStorage _symbolStorage;
        private readonly FileTxSupport _symbolTxSupport;
        private readonly CompositeRawFile _txLogFile;
        private readonly ITxLog _txLog;
        private readonly EPartitionType _partitionType;
        private ITransactionContext _lastTransactionLog;
        private readonly bool _asyncPartitionClose;

        public PartitionManager(IJournalMetadata<T> metadata, EFileAccess access,
            ICompositeFileFactory fileFactory, IJournalServer server, ITxLog txLog = null)
        {
            Access = access;
            _metadata = metadata;
            _settings = metadata.Settings;
            _fileFactory = fileFactory;
            _server = server;

            _symbolStorage = InitializeSymbolStorage();
            _symbolTxSupport = new FileTxSupport(SYMBOL_PARTITION_ID, _symbolStorage, _metadata);

            if (txLog == null)
            {
                var txFileName = Path.Combine(metadata.Settings.DefaultPath, MetadataConstants.TX_FILE_NAME);
                _txLogFile = new CompositeRawFile(txFileName,
                    MetadataConstants.PIPE_BIT_HINT, _fileFactory, access, SYMBOL_PARTITION_ID,
                    MetadataConstants.TX_LOG_FILE_ID, MetadataConstants.TX_LOG_FILE_ID, EDataType.Data);

                txLog = new TxLog(_txLogFile);
            }
            _txLog = txLog;

            var di = new DirectoryInfo(_settings.DefaultPath);
            if (!di.Exists)
            {
                di.Create();
            }
            ConfigurePartitionType();
            _partitionType = _settings.PartitionType;
            _asyncPartitionClose = _settings.PartitionCloseStrategy.HasFlag(
                EPartitionCloseStrategy.FullPartitionCloseAsynchronously);
        }

        public EFileAccess Access { get; private set; }

        public IEnumerable<IPartitionCore> GetOpenPartitions()
        {
            lock (_partitions)
            {
                return _partitions.ToArray();
            }
        }

        public IColumnStorage SymbolFileStorage
        {
            get { return _symbolStorage; }
        }

        public IPartition<T> GetPartitionByID(int partitionID)
        {
            // Partition IDs are 1 based. 
            // 0 is reserved for symbols "parition".
            return _partitions[partitionID - 1];
        }

        public ITransactionContext ReadTxLog()
        {
            lock (_partitions)
            {
                lock (_lastTransLogSync)
                {
                    // _tx file.
                    var txRec = _txLog.Get();

                    // Should be re-written using transaction log.
                    ReconcilePartitionsWithDisk(txRec);

                    var tx = new DeferredTransactionContext(_symbolTxSupport, _partitions, txRec);
                    if (txRec != null)
                    {
                        tx.PrevTxAddress = Math.Max(txRec.PrevTxAddress, TxLog.MIN_TX_ADDRESS) + txRec.Size();
                    }

                    // Set state to inital.
                    _lastTransactionLog = tx;
                    return tx;
                }
            }
        }

        public void Commit(ITransactionContext tx)
        {
            if (Access != EFileAccess.ReadWrite)
            {
                throw new NFSdbCommitFailedException(
                    "Journal opened in readonly mode. Transaction commit is not allowed");
            }

            lock (_partitions)
            {
                lock (_lastTransLogSync)
                {
                    var processedFiles = new List<IRollback>(_partitions.Count + 1);
                    var modified = _partitions
                        .Where(p => tx.IsParitionUpdated(p.PartitionID, _lastTransactionLog))
                        .ToArray();

                    // Non-empty tx.
                    if (modified.Any())
                    {
                        try
                        {
                            foreach (var txFile in modified)
                            {
                                processedFiles.Add(txFile.Commit(tx));
                            }
                            processedFiles.Add(_symbolTxSupport.Commit(tx));
                        }
                        catch (NFSdbCommitFailedException)
                        {
                            foreach (var rb in processedFiles)
                            {
                                rb.Rollback();
                            }
                            throw;
                        }

                        try
                        {
                            var lastPartition = _partitions[_partitions.Count - 1];
                            var rec = new TxRec();
                            lastPartition.SetTxRec(tx, rec);
                            _symbolTxSupport.SetTxRec(tx, rec);
                            _txLog.Create(rec);
                        }
                        catch (Exception ex)
                        {
                            throw new NFSdbCommitFailedException("Error writing _tx file", ex);
                        }
                    }
                    _lastTransactionLog = tx;
                }
            }
        }

        public IPartition<T> GetAppendPartition(DateTime dateTime, ITransactionContext tx)
        {
            var lastUsedPartition = tx.GetPartitionTx();
            if (tx.LastAppendTimestamp <= dateTime 
                && lastUsedPartition != null && _partitions.Count <= lastUsedPartition.ParitionID)
            {
                var part = _partitions[lastUsedPartition.ParitionID - 1];
                if (dateTime >= part.StartDate && dateTime < part.EndDate)
                {
                    return part;
                }
            }

            return GetAppendPartition0(dateTime, tx);
        }

        private IPartition<T> GetAppendPartition0(DateTime dateTime, ITransactionContext tx)
        {
            lock (_partitions)
            {
                if (tx.LastAppendTimestamp > dateTime)
                {
                    throw new NFSdbInvalidAppendException(
                            "Journal {0}. Attempt to insert a record out of order." +
                            " Record with timestamp {1} cannot be inserted when" +
                            " the last appended record's timestamp is {2}",
                            _metadata.Settings.DefaultPath,
                            tx.LastAppendTimestamp,
                            dateTime);
                }

                var startDate = PartitionManagerUtils.GetPartitionStartDate(dateTime,
                    _metadata.Settings.PartitionType);

                var dirName = PartitionManagerUtils.GetPartitionDirName(dateTime,
                    _metadata.Settings.PartitionType);

                var paritionDir = Path.Combine(_metadata.Settings.DefaultPath, dirName);

                // 0 reserved for symbols.
                var partitionID = _partitions.Count + 1;
                var partition = new Partition<T>(_metadata, _fileFactory, Access, startDate,
                    partitionID, paritionDir);

                _partitions.Add(partition);
                tx.AddPartition(partition);
                SwitchWritePartitionTo(tx, partitionID);

                return partition;
            }
        }

        private void SwitchWritePartitionTo(ITransactionContext tx, int partitionID)
        {
            var prevPartitionTx = tx.GetPartitionTx();
            tx.SetCurrentPartition(partitionID);

            if (prevPartitionTx != null)
            {
                // Write append offsets in all the files.
                var pp = _partitions[prevPartitionTx.ParitionID - 1];
                pp.Commit(tx);

                if (_asyncPartitionClose)
                {
                    pp.Commit(tx);
                    _server.SchedulePartitionAppendComplete(() => ClosePartitionAsync(prevPartitionTx.ParitionID));
                }
            }
        }

        private void ClosePartitionAsync(int partitionID)
        {
            IPartition<T> part = null;
            lock (_partitions)
            {
                int partInx = partitionID - 1;
                if (_partitions.Count > partInx)
                {
                    part = _partitions[partInx];
                    if (part.PartitionID != partitionID)
                    {
                        part = null;
                    }
                }
            }
            if (part != null)
            {
                part.CloseFiles();
            }
        }

        public void Truncate(ITransactionContext tx)
        {
            lock (_partitions)
            {
                foreach (var partition in _partitions)
                {
                    partition.Dispose();
                    Directory.Delete(partition.DirectoryPath, true);
                }
                _partitions.Clear();
            }
        }

        private ColumnStorage InitializeSymbolStorage()
        {
            var symbolStorage = new ColumnStorage(
                _metadata.Settings, _metadata.Settings.DefaultPath, 
                Access, SYMBOL_PARTITION_ID, _fileFactory);
            _metadata.InitializeSymbols(symbolStorage);
            return symbolStorage;
        }

        private void ReconcilePartitionsWithDisk(TxRec txRec, DirectoryInfo di = null)
        {
            var defaultPath = _settings.DefaultPath;
            di = di ?? new DirectoryInfo(defaultPath);

            var lastPartitionStart = DateTime.MinValue;
            var nextPartitionID = MetadataConstants.SYMBOL_PARTITION_ID + 1;

            // _partitions should be already locked here.
            while (_partitions.Count > 0)
            {
                var index = _partitions.Count - 1;
                var lastPartition = _partitions[index];
                nextPartitionID = lastPartition.PartitionID + 1;
                lastPartitionStart = lastPartition.StartDate;

                // Remove uncommited partitions
                if (!txRec.IsCommited(lastPartitionStart, lastPartition.PartitionID))
                {
                    var toRemove = _partitions[index];
                    toRemove.Dispose();
                    _partitions.RemoveAt(index);
                }
                else
                {
                    break;
                }
            }

            var subDirs = di.EnumerateDirectories().Select(d => d.Name).OrderBy(s => s);
            foreach (string subDir in subDirs)
            {
                if (!subDir.StartsWith(MetadataConstants.TEMP_DIRECTORY_PREFIX))
                {
                    DateTime? startDate = PartitionManagerUtils
                        .ParseDateFromDirName(subDir, _partitionType);

                    var fullPath = Path.Combine(defaultPath, subDir);
                    if (startDate.HasValue)
                    {
                        if (startDate.Value > lastPartitionStart
                            || (_partitions.Count == 0 && startDate.Value == DateTime.MinValue))
                        {
                            if (txRec.IsCommited(startDate.Value, nextPartitionID))
                            {
                                lastPartitionStart = startDate.Value;
                                _partitions.Add(new Partition<T>(_metadata,
                                    _fileFactory, Access, startDate.Value,
                                    nextPartitionID++, fullPath));
                            }
                            else
                            {
                                LOG.InfoFormat("Ignoring directory '{0}' for partition type '{1}' as fully rolled back partition.", 
                                    fullPath, _partitionType);
                                break;
                            }
                        }
                    }
                    else
                    {
                        LOG.WarnFormat("Invalid directory '{0}' for partition type '{1}'. " +
                                       "Will be ignored.", fullPath, _partitionType);
                    }
                }
            }
        }

        [Conditional("DEBUG")]
        // ReSharper disable once UnusedMember.Local
        private void DebugCheckParitionID()
        {
            // ReSharper disable once CSharpWarnings::CS0162
            if (SYMBOL_PARTITION_ID != 0)
            {
                throw new NFSdbConfigurationException("SYMBOL_PARTITION_ID supposed to be 0 but was " + SYMBOL_PARTITION_ID);
            }
        }

        private void ConfigurePartitionType()
        {
            var partitionType = ReadPartitionType();

            if (!partitionType.HasValue)
            {
                partitionType = _settings.PartitionType;
                WritePartitionType(partitionType.Value);
            }
            else
            {
                _settings.OverridePartitionType(partitionType.Value);
            }
        }

        private void WritePartitionType(EPartitionType value)
        {
            var path = Path.Combine(_settings.DefaultPath, MetadataConstants.PARTITION_TYPE_FILENAME);
            File.WriteAllText(path, value.ToString().ToUpper());
        }

        private EPartitionType? ReadPartitionType()
        {
            var path = Path.Combine(_settings.DefaultPath, MetadataConstants.PARTITION_TYPE_FILENAME);
            try
            {
                if (File.Exists(path))
                {
                    EPartitionType val;
                    if (Enum.TryParse(File.ReadAllText(path), true, out val))
                    {
                        return val;
                    }
                }
            }
            catch (IOException)
            {
            }
            return null;
        }

        public void Dispose()
        {
            lock (_partitions)
            {
                if (_txLogFile != null)
                {
                    _txLogFile.Dispose();
                }

                foreach (var partition in _partitions)
                {
                    partition.Dispose();
                }
                _partitions.Clear();

                _symbolStorage.Dispose();
            }
        }
    }
}