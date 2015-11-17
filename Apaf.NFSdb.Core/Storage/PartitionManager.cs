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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Apaf.NFSdb.Core.Collections;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Server;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    public class PartitionManager<T> : IPartitionManager<T>, IUnsafePartitionManager
    {
        // ReSharper disable once StaticFieldInGenericType
        private const int SYMBOL_PARTITION_ID = MetadataConstants.SYMBOL_PARTITION_ID;
        private readonly ICompositeFileFactory _fileFactory;
        private readonly IJournalServer _server;
        private readonly IJournalMetadataCore _metadata;
        private readonly List<IPartitionCore> _partitions = new List<IPartitionCore>();
        private readonly JournalSettings _settings;
        private readonly ColumnStorage _symbolStorage;
        private readonly FileTxSupport _symbolTxSupport;
        private readonly CompositeRawFile _txLogFile;
        private readonly ITxLog _txLog;
        private readonly EPartitionType _partitionType;
        private ITransactionContext _lastTransactionLog;
        private readonly ConcurrentBag<TxState> _resuableTxState = new ConcurrentBag<TxState>();
        private const int RESERVED_PARTITION_COUNT = 10;

        private TxRec _lastTxRec;

        public PartitionManager(IJournalMetadataCore metadata, EFileAccess access,
            ICompositeFileFactory fileFactory, IJournalServer server, ITxLog txLog = null)
        {
            Access = access;
            _metadata = metadata;
            _settings = metadata.Settings;
            _fileFactory = fileFactory;
            _server = server;

            _symbolStorage = InitializeSymbolStorage();
            _symbolTxSupport = new FileTxSupport(SYMBOL_PARTITION_ID, _symbolStorage, _metadata, DateTime.MinValue, DateTime.MinValue);

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
        }

        internal event Action OnDisposed;

        public EFileAccess Access { get; private set; }

        public IPartitionCore GetPartition(int paritionID)
        {
            return GetPartitionByID(paritionID);
        }

        public IPartitionCore[] GetOpenPartitions()
        {
            lock (_partitions)
            {
                return _partitions.Cast<IPartitionCore>().ToArray();
            }
        }

        public IColumnStorage SymbolFileStorage
        {
            get { return _symbolStorage; }
        }

        private IPartitionCore GetPartitionByID(int partitionID)
        {
            // Partition IDs are 1 based. 
            // 0 is reserved for symbols "parition".
            lock (_partitions)
            {
                return _partitions[partitionID];
            }
        }

        private int PartitionSize
        {
            get { return _partitions.Count; }
        }

        private void SetPartitionByID(int partitionID, IPartition partition)
        {
            lock (_partitions)
            {
                _partitions.SetToIndex(partitionID, partition);
            }
        }

        public ITransactionContext ReadTxLog()
        {
            return ReadTxLog(_metadata.PartitionTtl.Milliseconds);
        }

        public ITransactionContext ReadTxLog(int partitionTtlMs)
        {
            // _tx file.
            if (_lastTxRec == null || Access != EFileAccess.ReadWrite)
            {
                _lastTxRec = _txLog.Get();
            }

            var state = GetTxState();
            ReconcilePartitionsWithTxRec(state.Partitions, _lastTxRec);
            var tx = new DeferredTransactionContext(state, _symbolTxSupport, this, _lastTxRec, partitionTtlMs);

            if (_lastTxRec != null)
            {
                tx.PrevTxAddress = Math.Max(_lastTxRec.PrevTxAddress, TxLog.MIN_TX_ADDRESS) + _lastTxRec.Size();
            }

            // Set state to inital.
            _lastTransactionLog = tx;
            
            return tx;
        }

        private TxState GetTxState()
        {
            TxState state;
            if (!_resuableTxState.TryTake(out state))
            {
                state = new TxState();
                state.ReadContext = new ReadContext();
                int capacity = PartitionSize + RESERVED_PARTITION_COUNT;
                state.Partitions = new List<IPartitionCore>(capacity);
                state.Locks = new List<bool>();
                state.PartitionDataStorage = new PartitionTxData[capacity];
            }

            return state;
        }


        public void Commit(ITransactionContext tx, int partitionTtl)
        {
            if (Access != EFileAccess.ReadWrite)
            {
                throw new NFSdbCommitFailedException(
                    "Journal opened in readonly mode. Transaction commit is not allowed");
            }

            bool isUpdated = false;
            try
            {
                PartitionTxData lastAppendPartition = tx.GetPartitionTx();
                // Non-empty commit.
                if (lastAppendPartition != null)
                {
                    var lastPartitionID = lastAppendPartition.PartitionID;
                    for (int i = tx.Partitions.Count - 1; i >= 0; i--)
                    {
                        var partition = tx.Partitions[i];
                        if (partition != null)
                        {
                            if (tx.IsParitionUpdated(partition.PartitionID, _lastTransactionLog))
                            {
                                partition.Commit(tx);
                                tx.RemoveRef(partitionTtl);
                                isUpdated = true;
                            }
                        }
                    }

                    if (isUpdated)
                    {
                        _symbolTxSupport.Commit(tx);

                        var lastPartition = GetPartitionByID(lastPartitionID);
                        var rec = new TxRec();
                        lastPartition.SetTxRec(tx, rec);
                        _symbolTxSupport.SetTxRec(tx, rec);
                        _txLog.Create(rec);
                        _lastTxRec = rec;
                    }
                }
            }
            catch (Exception ex)
            {
                throw new NFSdbCommitFailedException(
                    "Error commiting transaction. See InnerException for details.", ex);
            }
            _lastTransactionLog = tx;
        }

        public IPartitionCore GetAppendPartition(DateTime dateTime, ITransactionContext tx)
        {
            var lastUsedPartition = tx.GetPartitionTx();
            if (tx.LastAppendTimestamp <= dateTime && lastUsedPartition != null)
            {
                var part = GetPartitionByID(lastUsedPartition.PartitionID);
                if (part != null && dateTime >= part.StartDate && dateTime < part.EndDate)
                {
                    return part;
                }
            }

            return GetAppendPartition0(dateTime, tx);
        }

        private IPartition GetAppendPartition0(DateTime dateTime, ITransactionContext tx)
        {
            if (tx.LastAppendTimestamp > dateTime)
            {
                throw new NFSdbInvalidAppendException(
                    "Journal {0}. Attempt to insert a record out of order." +
                    " Record with timestamp {1} cannot be inserted when" +
                    " the last appended record's timestamp is {2}",
                    _metadata.Settings.DefaultPath,
                    dateTime,
                    tx.LastAppendTimestamp);
            }

            int lastPartitionID;

            // 0 partition index / ID is symobol partition.
            if (tx.Partitions.Count > 1)
            {
                var lastPart = tx.Partitions.LastNotNull();
                lastPartitionID = lastPart.PartitionID;

                if (lastPart.IsInsidePartition(dateTime))
                {
                    SwitchWritePartitionTo(tx, lastPart.PartitionID);
                    return (IPartition)lastPart;
                }
            }
            else
            {
                lastPartitionID = 0;
            }

            ClearPartitionsAfter(lastPartitionID);
            lastPartitionID++;
            var startDate = PartitionManagerUtils.GetPartitionStartDate(dateTime,
                _metadata.Settings.PartitionType);

            var dirName = PartitionManagerUtils.GetPartitionDirName(dateTime,
                _metadata.Settings.PartitionType);

            var paritionDir = Path.Combine(_metadata.Settings.DefaultPath, dirName);

            // 0 reserved for symbols.
            var partition = new Partition(_metadata, _fileFactory, Access, startDate,
                lastPartitionID, paritionDir, _server);

            SetPartitionByID(lastPartitionID, partition);

            tx.AddPartition(partition);
            SwitchWritePartitionTo(tx, lastPartitionID);

            return partition;
        }

        private void ClearPartitionsAfter(int lastPartitionID)
        {
            lock (_partitions)
            {
                for (int i = lastPartitionID + 1; i < _partitions.Count; i++)
                {
                    var rollbackPartition = GetPartitionByID(i);
                    if (rollbackPartition != null)
                    {
                        rollbackPartition.Dispose();
                        if (Directory.Exists(rollbackPartition.DirectoryPath))
                        {
                            Directory.Delete(rollbackPartition.DirectoryPath, true);
                        }
                    }
                }
            }
        }

        private void SwitchWritePartitionTo(ITransactionContext tx, int partitionID)
        {
            var prevPartitionTx = tx.GetPartitionTx();
            tx.SetCurrentPartition(partitionID);
            tx.Partitions[partitionID].AddRef();

            if (prevPartitionTx != null)
            {
                // Write append offsets in all the files.
                var pp = GetPartitionByID(prevPartitionTx.PartitionID);
                pp.Commit(tx);

                var deref = tx.Partitions[prevPartitionTx.PartitionID];
                if (deref != null)
                {
                    tx.RemoveRef(prevPartitionTx.PartitionID);
                }
            }
        }

        private ColumnStorage InitializeSymbolStorage()
        {
            var symbolStorage = new ColumnStorage(
                _metadata, _metadata.Settings.DefaultPath, 
                Access, SYMBOL_PARTITION_ID, _fileFactory);
            _metadata.InitializeSymbols(symbolStorage);
            return symbolStorage;
        }

        private void ReconcilePartitionsWithTxRec(List<IPartitionCore> partitions, TxRec txRec)
        {
            partitions.Clear();

            var defaultPath = _settings.DefaultPath;

            for (int i = 0; i < PartitionSize; i++)
            {
                var partition = GetPartitionByID(i);
                if (partition != null)
                {
                    if (txRec.IsCommited(partition.StartDate, i))
                    {
                        partitions.SetToIndex(i, partition);
                    }
                    else
                    {
                        break;
                    }
                }
            }

            if (PartitionSize == 0 || Access != EFileAccess.ReadWrite)
            {
                AddNewPartitions(txRec, defaultPath, partitions);
            }
        }

        private void AddNewPartitions(TxRec txRec, string defaultPath, List<IPartitionCore> partitions)
        {
            var nextPartitionID = 1;
            var lastPartitionStart = DateTime.MinValue;
            if (partitions.Count > 0)
            {
                var lastPartition = partitions.FindLast(p => p != null);
                nextPartitionID = lastPartition.PartitionID + 1;
                lastPartitionStart = lastPartition.StartDate;
            }
            else
            {
                // Symbols.
                partitions.Add(null);
            }
            var di = new DirectoryInfo(defaultPath);

            var subDirs = di.EnumerateDirectories().Select(d => d.Name).OrderBy(s => s);
            foreach (string subDir in subDirs)
            {
                if (!subDir.StartsWith(MetadataConstants.TEMP_DIRECTORY_PREFIX))
                {
                    var dateFromName = PartitionManagerUtils.ParseDateFromDirName(subDir, _partitionType);
                    var fullPath = Path.Combine(defaultPath, subDir);

                    if (dateFromName.HasValue)
                    {
                        var startDate = dateFromName.Value;
                        if (startDate > lastPartitionStart || startDate == DateTime.MinValue)
                        {
                            if (txRec.IsCommited(startDate, nextPartitionID))
                            {
                                lastPartitionStart = startDate;
                                var partition = new Partition(_metadata, _fileFactory, Access, startDate, nextPartitionID, fullPath, _server);

                                SetPartitionByID(nextPartitionID, partition);
                                
                                partitions.SetToIndex(nextPartitionID, partition);
                                nextPartitionID++;
                            }
                            else
                            {
                                Trace.TraceInformation(
                                    "Ignoring directory '{0}' for partition type '{1}' as fully rolled back partition.",
                                    fullPath, _partitionType);
                                break;
                            }
                        }
                    }
                    else
                    {
                        Trace.TraceWarning("Invalid directory '{0}' for partition type '{1}'. " +
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
                    if (partition != null)
                    {
                        partition.Dispose();
                    }
                }
                _partitions.Clear();

                _symbolStorage.Dispose();

                if (OnDisposed != null)
                {
                    try
                    {
                        OnDisposed();
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceError("Error calling OnDisposed event " + ex);
                    }
                }
            }
        }

        public IPartitionReader Read(int paritionID)
        {
            return GetPartitionByID(paritionID);
        }

        public void Recycle(TxState state)
        {
            if (state != null)
            {
                _resuableTxState.Add(state);
            }
        }

        public void DetachPartition(int partitionID)
        {
            _lastTransactionLog = null;
            SetPartitionByID(partitionID, null);
        }

        public void ClearTxLog()
        {
            _lastTransactionLog = null;
            _lastTxRec = null;
            _txLog.Clean();
        }
    }
}