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
    public class PartitionManager : IPartitionManager, IUnsafePartitionManager
    {
        // ReSharper disable once StaticFieldInGenericType
        private const int SYMBOL_PARTITION_ID = MetadataConstants.SYMBOL_PARTITION_ID;
        private readonly ICompositeFileFactory _fileFactory;
        private readonly IJournalServer _server;
        private readonly IJournalMetadata _metadata;
        private readonly ConcurrentDictionary<DateTime, Lazy<IPartition>> _partitions = new ConcurrentDictionary<DateTime, Lazy<IPartition>>();
        private readonly ConcurrentQueue<IPartition> _allPartitions = new ConcurrentQueue<IPartition>();
        private readonly JournalSettings _settings;
        private readonly ColumnStorage _symbolStorage;
        private readonly FileTxSupport _symbolTxSupport;
        private readonly CompositeRawFile _txLogFile;
        private readonly ITxLog _txLog;
        private ITransactionContext _lastTransactionLog;
        private readonly ConcurrentBag<TxState> _resuableTxState = new ConcurrentBag<TxState>();
        private const int RESERVED_PARTITION_COUNT = 10;

        private TxRec _lastTxRec;

        public PartitionManager(IJournalMetadata metadata, EFileAccess access,
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
        }

        internal event Action OnDisposed;

        public EFileAccess Access { get; private set; }
        
        public IPartition[] GetOpenPartitions()
        {
            lock (_partitions)
            {
                return _partitions.Values.Where(p => p.IsValueCreated)
                    .Select(p => p.Value).ToArray();
            }
        }

        public IColumnStorage SymbolFileStorage
        {
            get { return _symbolStorage; }
        }

        private IPartition GetPartitionByID(ITransactionContext tx, int partitionID)
        {
            return tx.Partitions[partitionID];
        }

        private int PartitionSize
        {
            get { return _partitions.Count; }
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
            tx.AddRefsAllPartitions();

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
                state.Partitions = new List<IPartition>(capacity);
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

                        var lastPartition = GetPartitionByID(tx, lastPartitionID);
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

        public IPartition GetAppendPartition(DateTime dateTime, ITransactionContext tx)
        {
            var lastUsedPartition = tx.GetPartitionTx();
            if (tx.LastAppendTimestamp <= dateTime && lastUsedPartition != null)
            {
                var part = GetPartitionByID(tx, lastUsedPartition.PartitionID);
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
                    return lastPart;
                }
            }
            else
            {
                lastPartitionID = 0;
            }

            ClearPartitionsAfter(lastPartitionID, tx);
            lastPartitionID++;
            var startDate = PartitionManagerUtils.GetPartitionStartDate(dateTime,
                _metadata.Settings.PartitionType);

            var dirName = PartitionManagerUtils.GetPartitionDirName(dateTime,
                _metadata.Settings.PartitionType);

            var paritionDir = Path.Combine(_metadata.Settings.DefaultPath, dirName);

            // 0 reserved for symbols.
            var partition = CreateNewParition(
                new PartitionDate(startDate, 0, _settings.PartitionType), lastPartitionID,
                paritionDir);

            var lazy = new Lazy<IPartition>(() => partition);
            _partitions[startDate] = lazy;

            // Force create lazy value.
            tx.AddPartition(lazy.Value);
            SwitchWritePartitionTo(tx, lastPartitionID);

            return partition;
        }

        private void ClearPartitionsAfter(int lastPartitionID, ITransactionContext tx)
        {
            lock (_partitions)
            {
                for (int i = lastPartitionID + 1; i < tx.Partitions.Count; i++)
                {
                    var rollbackPartition = tx.Partitions[i];
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
            tx.AddRef(partitionID);

            if (prevPartitionTx != null)
            {
                // Write append offsets in all the files.
                var pp = GetPartitionByID(tx, prevPartitionTx.PartitionID);
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

        private void ReconcilePartitionsWithTxRec(List<IPartition> partitions, TxRec txRec)
        {
            partitions.Clear();
            var defaultPath = _settings.DefaultPath;
            
            // Symbols are the partition 0.
            partitions.Add(null);
            var nextPartitionID = 1;

            var subDirs = LatestPartitionVersions(defaultPath);
            foreach (var subDir in subDirs)
            {
                var fullPath = Path.Combine(defaultPath, subDir.Name);
                var startDate = subDir.Date;
                var partitionDir = subDir;
                if (txRec.IsCommited(startDate, nextPartitionID))
                {
                    var partitionID = nextPartitionID;
                    var partition = _partitions.AddOrUpdate(startDate,
                        // Add.
                        sd => new Lazy<IPartition>(() => CreateNewParition(partitionDir, partitionID, fullPath)),
                        // Update.
                        (sd, existing) =>
                        {
                            if (existing.Value.Version == subDir.Version) return existing;
                            existing.Value.MarkOverwritten();
                            return new Lazy<IPartition>(() => CreateNewParition(partitionDir, partitionID, fullPath));
                        });


                    partitions.SetToIndex(partitionID, partition.Value);
                    nextPartitionID++;
                }
                else
                {
                    Trace.TraceInformation(
                        "Ignoring directory '{0}' for partition type '{1}' as fully rolled back partition.",
                        fullPath, _settings.PartitionType);

                    Lazy<IPartition> existingPartition;
                    if (_partitions.TryRemove(startDate, out existingPartition))
                    {
                        if (existingPartition.IsValueCreated)
                        {
                            existingPartition.Value.Dispose();
                        }
                    }
                }
            }
        }

        private IPartition CreateNewParition(PartitionDate partitionDir, int partitionID, string fullPath)
        {
            var newParition = new Partition(_metadata, _fileFactory, Access, partitionDir, partitionID, fullPath, _server);
            _allPartitions.Enqueue(newParition);
            return newParition;
        }

        private IEnumerable<PartitionDate> LatestPartitionVersions(string defaultPath)
        {
            var di = new DirectoryInfo(defaultPath);
            var subDirs = di.EnumerateDirectories().Select(d => d.Name).OrderBy(s => s);
            PartitionDate? lastDate = null;
            foreach (var subDir in subDirs)
            {
                if (!subDir.StartsWith(MetadataConstants.TEMP_DIRECTORY_PREFIX))
                {
                    var dateFromName = PartitionManagerUtils.ParseDateFromDirName(subDir, _settings.PartitionType);
                    var fullPath = Path.Combine(defaultPath, subDir);

                    if (dateFromName.HasValue)
                    {
                        if (lastDate.HasValue && !lastDate.Value.Date.Equals(dateFromName.Value.Date))
                        {
                            yield return lastDate.Value;
                        }
                        lastDate = dateFromName;
                    }
                    else
                    {
                        Trace.TraceWarning("Invalid directory '{0}' for partition type '{1}'. " +
                                           "Will be ignored.", fullPath, _settings.PartitionType);
                    }
                }
            }
            if (lastDate.HasValue)
            {
                yield return lastDate.Value;
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

        public void Dispose()
        {
            if (_txLogFile != null)
            {
                _txLogFile.Dispose();
            }

            foreach (var partition in _allPartitions)
            {
                partition.Dispose();
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

        public void Recycle(TxState state)
        {
            if (state != null)
            {
                _resuableTxState.Add(state);
            }
        }

        public void ClearTxLog()
        {
            _lastTransactionLog = null;
            _lastTxRec = null;
            _txLog.Clean();
        }
    }
}