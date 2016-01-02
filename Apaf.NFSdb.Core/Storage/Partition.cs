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
using System.Threading;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Server;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core.Storage
{
    public class Partition : IPartition
    {
        private readonly ICompositeFileFactory _memeorymMappedFileFactory;
        private readonly EFileAccess _access;
        private readonly IJournalServer _journalServer;
        private IFieldSerializer _fieldSerializer;
        private ColumnStorage _columnStorage;
        private FileTxSupport _txSupport;
        private IFixedWidthColumn _timestampColumn;
        private readonly IJournalMetadata _metadata;
        private bool _isStorageInitialized;
        private readonly object _syncRoot = new object();
        private int _refCount;
        private ColumnSource[] _columns;
        private readonly PartitionDate _partitionDate;
        private bool _isOverwritten;

        public Partition(IJournalMetadata metadata,
            ICompositeFileFactory memeorymMappedFileFactory,
            EFileAccess access,
            PartitionDate partitionDate, int partitionID,
            string path, 
            IJournalServer journalServer)
        {
            _memeorymMappedFileFactory = memeorymMappedFileFactory;
            _access = access;
            _journalServer = journalServer;
            _metadata = metadata;

            _partitionDate = partitionDate;
            EndDate = PartitionManagerUtils.GetPartitionEndDate(
                partitionDate.Date, partitionDate.PartitionType);
            PartitionID = partitionID;
            DirectoryPath = path;
        }

        public DateTime StartDate
        {
            get { return _partitionDate.Date; }
        }

        public int Version
        {
            get { return _partitionDate.Version; }
        }

        public DateTime EndDate { get; private set; }

        public int GetOpenFileCount()
        {
            if (_isStorageInitialized)
            {
                int mappedSize = 0;
                for (int i = 0; i < _columnStorage.OpenFileCount; i++)
                {
                    var file = _columnStorage.GetOpenedFileByID(i);
                    if (file != null && file.MappedSize > 0)
                    {
                        mappedSize++;
                    }
                }
                return mappedSize;
            }
            return 0;
        }

        public long GetTotalMemoryMapped()
        {
            if (_isStorageInitialized)
            {
                long mappedSize = 0;
                for (int i = 0; i < _columnStorage.OpenFileCount; i++)
                {
                    var file = _columnStorage.GetOpenedFileByID(i);
                    if (file != null)
                    {
                        mappedSize += file.MappedSize;
                    }
                }
                return mappedSize;
            }
            return 0L;
        }

        public int PartitionID { get; private set; }
        public string DirectoryPath { get; private set; }

        public TT Read<TT>(long rowID, IReadContext readContext)
        {
            if (!_isStorageInitialized) InitializeStorage();

            return (TT)_fieldSerializer.Read(rowID, readContext);
        }

        public void Append(object item, ITransactionContext tx)
        {
            if (!_isStorageInitialized) InitializeStorage();

            var pd = tx.GetPartitionTx();
            _fieldSerializer.Write(item, pd.NextRowID, tx);
            pd.NextRowID++;
            pd.IsAppended = true;
        }

        public void MarkOverwritten()
        {
            _isOverwritten = true;
            if (Interlocked.CompareExchange(ref _refCount, -1, 0) == 0)
            {
                TryDeletePartitionDirectory();

                // Unlock.
                _refCount = 0;
            }
        }

        public bool IsOverwritten
        {
            get { return _isOverwritten; }
        }

        public void TryCloseFiles()
        {
            // Try locking.
            var localCopy = _columnStorage;
            if (localCopy == null) return;

            if (Interlocked.CompareExchange(ref _refCount, -1, 0) == 0)
            {
                _isStorageInitialized = false;

                // Unlock.
                _refCount = 0;
            }
            else
            {
                return;
            }

            Thread.MemoryBarrier();
            localCopy.Dispose();

            if (_isOverwritten)
            {
                TryDeletePartitionDirectory();
            }
        }

        private void TryDeletePartitionDirectory()
        {
            if (_access != EFileAccess.ReadWrite)
            {
                return;
            }

            try
            {
                Directory.Delete(DirectoryPath, true);
            }
            catch (IOException ex)
            {
                Trace.TraceWarning("Error deleting outdated partition {0}. Error {1}", DirectoryPath, ex);
            }
            catch (UnauthorizedAccessException ex)
            {
                Trace.TraceWarning("Error deleting outdated partition {0}. Error {1}", DirectoryPath, ex);
            }
        }

        public int AddRef()
        {
            int localRefCount;
            do
            {
                localRefCount = _refCount;
                // -1 means exclusive mode.
                // Spin wait then.
                if (_refCount == -1)
                {
                    Thread.Yield();
                }
            } while (localRefCount == -1 ||
                Interlocked.CompareExchange(ref _refCount, localRefCount + 1, localRefCount) != localRefCount);

            return localRefCount + 1;
        }

        public int RemoveRef(int partitionOffloadMs)
        {
            var count = Interlocked.Decrement(ref _refCount);
            if (count == 0)
            {
                _journalServer.SignalUnusedPartition(this, partitionOffloadMs);
            }
            else if (count < 0)
            {
                throw new NFSdbInvalidStateException("Partition '{0}' ref count is negative '{1}' outside of lock.",
                    DirectoryPath, count);
            }
            return count;
        }

        public void Dispose()
        {
            var colStorage = _columnStorage;
            if (colStorage != null)
            {
                colStorage.Dispose();
            }
        }

        public PartitionTxData ReadTxLogFromPartition(TxRec txRec = null)
        {
            if (!_isStorageInitialized) InitializeStorage();

            return _txSupport.ReadTxLogFromPartition(txRec);
        }

        public IRollback Commit(ITransactionContext tx)
        {
            if (!_isStorageInitialized) return null;

            // Set respective append offset.
            // Some serializers can skip null fields.
            var pd = tx.GetPartitionTx(PartitionID);
            var count = pd.NextRowID;

            for (int i = 0; i < _columnStorage.OpenFileCount; i++)
            {
                var file = _columnStorage.GetOpenedFileByID(i);
                if (file != null)
                {
                    var column = _metadata.GetColumnByID(file.ColumnID);
                    var size = StorageSizeUtils.GetRecordSize(column, file.DataType);
                    if (size > 0)
                    {
                        pd.AppendOffset[file.FileID] = count * size;
                    }
                }
            }
            return _txSupport.Commit(tx);
        }

        public void SetTxRec(ITransactionContext tx, TxRec rec)
        {
            _txSupport.SetTxRec(tx, rec);
        }

        public long BinarySearchTimestamp(DateTime value, IReadTransactionContext tx)
        {
            if (!_isStorageInitialized) InitializeStorage();

            if (_timestampColumn == null)
            {
                throw new NFSdbConfigurationException("timestampColumn is not configured for journal in "
                                                      + DirectoryPath);
            }

            var hi = tx.GetRowCount(PartitionID);
            var timestampType = _timestampColumn.FieldType;
            long values = timestampType == EFieldType.Int64
                     || timestampType == EFieldType.DateTimeEpochMs
                ? DateUtils.DateTimeToUnixTimeStamp(value)
                : DateUtils.ToUnspecifiedDateTicks(value);
            
            return ColumnValueBinarySearch.LongBinarySerach(_timestampColumn, values, 0L, hi);
        }

        public IEnumerable<long> GetSymbolRows<TT>(int fileID, TT value, IReadTransactionContext tx)
        {
            if (!_isStorageInitialized) InitializeStorage();
            var symb = (IIndexedColumn<TT>)(_columns[fileID].Column);
            var key = symb.CheckKeyQuick(value, tx);
            return symb.GetValues(key, tx);
        }

        public int GetSymbolKey<TT>(int fieldID, TT value, IReadTransactionContext tx)
        {
            if (!_isStorageInitialized) InitializeStorage();
            var symb = (IIndexedColumn<TT>)(_columns[fieldID].Column);
            var key = symb.CheckKeyQuick(value, tx);
            return key;
        }

        public IEnumerable<long> GetSymbolRowsByKey(int fieldID, int valueKey, IReadTransactionContext tx)
        {
            if (!_isStorageInitialized) InitializeStorage();

            var symb = (IIndexedColumnCore)(_columns[fieldID].Column);
            return symb.GetValues(valueKey, tx);
        }

        public IColumn ReadColumn(int columnID)
        {
            if (!_isStorageInitialized) InitializeStorage();
            return _columns[columnID].Column;
        }

        public long GetSymbolRowCount<TT>(int fieldID, TT value, IReadTransactionContext tx)
        {
            if (!_isStorageInitialized) return 0;

            var symb = (IIndexedColumn<TT>)(_columns[fieldID].Column);
            var key = symb.CheckKeyQuick(value, tx);
            return symb.GetCount(key, tx);
        }

        private void InitializeStorage()
        {
            lock (_syncRoot)
            {
                if (!_isStorageInitialized)
                {
                    _columnStorage = new ColumnStorage(_metadata, DirectoryPath,
                        _access, PartitionID, _memeorymMappedFileFactory);

                    _columns = _metadata.GetPartitionColums(_columnStorage).ToArray();

                    if (_metadata.TimestampColumnID.HasValue)
                    {
                        _timestampColumn =
                            (IFixedWidthColumn)_columns[_metadata.TimestampColumnID.Value].Column;
                    }

                    _fieldSerializer = _metadata.GetSerializer(_columns);
                    _txSupport = new FileTxSupport(PartitionID, _columnStorage, _metadata, StartDate, EndDate);

                    Thread.MemoryBarrier();
                    _isStorageInitialized = true;
                }
            }
        }
    }
}