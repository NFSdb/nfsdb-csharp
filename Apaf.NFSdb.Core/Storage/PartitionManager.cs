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
        private readonly ColumnStorage _symbolStorage;
        private readonly FileTxSupport _symbolTxSupport;
        private readonly CompositeRawFile _txLogFile;
        private readonly ITxLog _txLog;
        private readonly EPartitionType _partitionType;
        private ITransactionContext _lastTransactionLog;
        private readonly bool _asyncPartitionClose;
        private readonly ConcurrentBag<TxReusableState> _resuableTxState = new ConcurrentBag<TxReusableState>();
        private const int RESERVED_PARTITION_COUNT = 10;

        private readonly IPartitionRangeLock _partitionsLock = new PartitionRangeLock(0);
        private TxRec _lastTxRec;

        public PartitionManager(IJournalMetadata<T> metadata, EFileAccess access,
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

        private IPartition<T> GetPartitionByID(int partitionID)
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

        private void SetPartitionByID(int partitionID, Partition<T> partition)
        {
            lock (_partitions)
            {
                while (partitionID >= _partitions.Count)
                {
                    _partitions.Add(null);
                }
                var existing = _partitions[partitionID];
                if (existing != null)
                {
                    existing.Dispose();
                }
                _partitions[partitionID] = partition;
            }
        }

        public ITransactionContext ReadTxLog()
        {
            lock (_partitions)
            {
                // _tx file.
                if (_lastTxRec == null || Access != EFileAccess.ReadWrite)
                {
                    _lastTxRec = _txLog.Get();
                }

                var state = GetTxState();
                ReconcilePartitionsWithTxRec(state.PartitionIDs, _lastTxRec);
                var tx = new DeferredTransactionContext(state, this, _lastTxRec);

                if (_lastTxRec != null)
                {
                    tx.PrevTxAddress = Math.Max(_lastTxRec.PrevTxAddress, TxLog.MIN_TX_ADDRESS) + _lastTxRec.Size();
                }

                // Set state to inital.
                _lastTransactionLog = tx;
                return tx;
            }
        }

        private TxReusableState GetTxState()
        {
            TxReusableState state;
            if (!_resuableTxState.TryTake(out state))
            {
                state = new TxReusableState();
                state.PartitionLock = new SaveLockWrapper(_partitionsLock);
                state.ReadContext = new ReadContext();
                int capacity = PartitionSize + RESERVED_PARTITION_COUNT;
                state.PartitionIDs = new List<int>(capacity);
                state.PartitionDataStorage = new PartitionTxData[capacity];
            }

            state.PartitionLock.Initialize(PartitionSize == 0 ? RESERVED_PARTITION_COUNT : PartitionSize);

            return state;
        }


        public void Commit(ITransactionContext tx)
        {
            if (Access != EFileAccess.ReadWrite)
            {
                throw new NFSdbCommitFailedException(
                    "Journal opened in readonly mode. Transaction commit is not allowed");
            }

            bool isUpdated = false;
            bool closeOnCommit =
                _settings.PartitionCloseStrategy.HasFlag(EPartitionCloseStrategy.CloseFullPartitionOnCommit);
            try
            {
                PartitionTxData lastAppendPartition = tx.GetPartitionTx();
                // Non-empty commit.
                if (lastAppendPartition != null)
                {
                    var lastPartitionID = lastAppendPartition.PartitionID;
                    for (int i = tx.PartitionIDs.Count - 1; i >= 0; i--)
                    {
                        int partitionID = tx.PartitionIDs[i];
                        var partition = GetPartitionByID(partitionID);
                        if (partition != null &&
                            tx.IsParitionUpdated(partition.PartitionID, _lastTransactionLog))
                        {
                            partition.Commit(tx);

                            // If this is not the last written partition
                            // consider to close.
                            if (partitionID != lastPartitionID && closeOnCommit && isUpdated)
                            {
                                partition.CloseFiles();
                            }
                            isUpdated = true;
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

        public IPartition<T> GetAppendPartition(DateTime dateTime, ITransactionContext tx)
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

        private IPartition<T> GetAppendPartition0(DateTime dateTime, ITransactionContext tx)
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

            int lastPartitionID;
            if (tx.PartitionIDs.Count > 0)
            {
                lastPartitionID = tx.PartitionIDs.Last();
                var lastPart = GetPartitionByID(lastPartitionID);
                if (lastPart.IsInsidePartition(dateTime))
                {
                    SwitchWritePartitionTo(tx, lastPart.PartitionID);
                    return lastPart;
                }
            }
            else
            {
                lastPartitionID = 0;
            }
            lastPartitionID++;
            var startDate = PartitionManagerUtils.GetPartitionStartDate(dateTime,
                _metadata.Settings.PartitionType);

            var dirName = PartitionManagerUtils.GetPartitionDirName(dateTime,
                _metadata.Settings.PartitionType);

            var paritionDir = Path.Combine(_metadata.Settings.DefaultPath, dirName);

            // 0 reserved for symbols.
            var partition = new Partition<T>(_metadata, _fileFactory, Access, startDate,
                lastPartitionID, paritionDir);

            SetPartitionByID(lastPartitionID, partition);
            _partitionsLock.SizeToPartitionID(lastPartitionID);

            tx.AddPartition(lastPartitionID);
            SwitchWritePartitionTo(tx, lastPartitionID);

            return partition;
        }

        private void SwitchWritePartitionTo(ITransactionContext tx, int partitionID)
        {
            var prevPartitionTx = tx.GetPartitionTx();
            tx.SetCurrentPartition(partitionID);

            if (prevPartitionTx != null)
            {
                // Write append offsets in all the files.
                var pp = GetPartitionByID(prevPartitionTx.PartitionID);
                pp.Commit(tx);

                if (_asyncPartitionClose)
                {
                    pp.Commit(tx);
                    _server.SchedulePartitionAppendComplete(() => ClosePartitionAsync(prevPartitionTx.PartitionID));
                }
            }
        }

        private void ClosePartitionAsync(int partitionID)
        {
            IPartition<T> part = GetPartitionByID(partitionID);
            if (part.PartitionID != partitionID)
            {
                part = null;
            }

            if (part != null)
            {
                part.CloseFiles();
            }
        }

        public void Truncate(ITransactionContext tx)
        {
            throw new NotImplementedException();

//            lock (_partitions)
//            {
//                lock (_lastTransLogSync)
//                {
//                    _txLog.Create(new TxRec());
//
//                    foreach (var partition in _partitions)
//                    {
//                        partition.Dispose();
//                        Directory.Delete(partition.DirectoryPath, true);
//                    }
//                    _partitions.Clear();
//                    _lastTransactionLog = null;
//                }
//
//            }
        }

        private ColumnStorage InitializeSymbolStorage()
        {
            var symbolStorage = new ColumnStorage(
                _metadata.Settings, _metadata.Settings.DefaultPath, 
                Access, SYMBOL_PARTITION_ID, _fileFactory);
            _metadata.InitializeSymbols(symbolStorage);
            return symbolStorage;
        }

        private void ReconcilePartitionsWithTxRec(List<int> partitionIDs, TxRec txRec)
        {
            partitionIDs.Clear();
            var defaultPath = _settings.DefaultPath;

            for (int i = 0; i < PartitionSize; i++)
            {
                var partition = GetPartitionByID(i);
                if (partition != null)
                {
                    if (txRec.IsCommited(partition.StartDate, i))
                    {
                        partitionIDs.Add(i);
                    }
                    else
                    {
                        break;
                    }
                }
            }

            // One off.
            if (PartitionSize == 0 || Access != EFileAccess.ReadWrite)
            {
                AddNewPartitions(txRec, defaultPath, partitionIDs);
            }
        }

        private void AddNewPartitions(TxRec txRec, string defaultPath, List<int> partitionIds)
        {
            var nextPartitionID = 1;
            var lastPartitionStart = DateTime.MinValue;
            if (partitionIds.Count > 0)
            {
                int lastPartitionID = partitionIds.Last();
                var lastPartition = GetPartitionByID(lastPartitionID);
                nextPartitionID = lastPartitionID + 1;
                lastPartitionStart = lastPartition.StartDate;
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
                                _partitionsLock.SizeToPartitionID(nextPartitionID);
                                lastPartitionStart = startDate;
                                SetPartitionByID(nextPartitionID, new Partition<T>(_metadata,
                                    _fileFactory, Access, startDate, nextPartitionID, fullPath));

                                partitionIds.Add(nextPartitionID);
                                nextPartitionID++;
                            }
                            else
                            {
                                LOG.InfoFormat(
                                    "Ignoring directory '{0}' for partition type '{1}' as fully rolled back partition.",
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
                    if (partition != null)
                    {
                        partition.Dispose();
                    }
                }
                _partitions.Clear();

                _symbolStorage.Dispose();
            }
        }
//
//        public PartitionTxData GetPartitionTx(int partitionID, TxRec txRec)
//        {
//            if (partitionID == 0)
//            {
//                return _symbolTxSupport.ReadTxLogFromPartition(txRec);
//            }
//
//            lock (_partitions)
//            {
//                return _partitions[partitionID - 1].ReadTxLogFromPartition(txRec);
//            }
//        }

        public IPartitionReader Read(int paritionID)
        {
            return GetPartitionByID(paritionID);
        }

        public IFileTxSupport ReadTx(int paritionID)
        {
            if (paritionID == 0) return _symbolTxSupport;
            return GetPartitionByID(paritionID);
        }

        public void Recycle(TxReusableState state)
        {
            if (state != null)
            {
                _resuableTxState.Add(state);
            }
        }
    }
}