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
using System.Linq;
using System.Threading;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core.Storage
{
    public class Partition<T> : IPartition<T>
    {
        private readonly ICompositeFileFactory _memeorymMappedFileFactory;
        private readonly EFileAccess _access;
        private IFieldSerializer _fieldSerializer;
        private ColumnStorage _columnStorage;
        private FileTxSupport _txSupport;
        private IFixedWidthColumn _timestampColumn;
        private Dictionary<string, ISymbolMapColumn> _symbols;
        private readonly IJournalMetadata<T> _metadata;
        private bool _isStorageInitialized;
        private readonly object _syncRoot = new object();

        public Partition(IJournalMetadata<T> metadata,
            ICompositeFileFactory memeorymMappedFileFactory,
            EFileAccess access,
            DateTime startDate, int partitionID,
            string path)
        {
            _memeorymMappedFileFactory = memeorymMappedFileFactory;
            _access = access;
            _metadata = metadata;

            StartDate = startDate;
            EndDate = PartitionManagerUtils.GetPartitionEndDate(
                startDate, metadata.Settings.PartitionType);
            PartitionID = partitionID;
            DirectoryPath = path;
        }


        public IColumnStorage Storage { get { return _columnStorage; } }
        public DateTime StartDate { get; private set; }
        public DateTime EndDate { get; private set; }
        public int PartitionID { get; private set; }
        public string DirectoryPath { get; private set; }

        public T Read(long rowID, IReadContext readContext)
        {
            if (!_isStorageInitialized) InitializeStorage();

            return (T)_fieldSerializer.Read(rowID, readContext);
        }

        public void Append(T item, ITransactionContext tx)
        {
            if (!_isStorageInitialized) InitializeStorage();

            // Current partition.
            tx.SetCurrentPartition(PartitionID);

            var pd = tx.GetPartitionTx();
            tx.SetCurrentPartition(PartitionID);
            _fieldSerializer.Write(item, pd.NextRowID, tx);
            pd.NextRowID++;
            pd.IsAppended = true;
        }

        public void Dispose()
        {
            if (_columnStorage != null)
            {
                _columnStorage.Dispose();
            }
        }

        public PartitionTxData ReadTxLogFromPartition(TxRec txRec = null)
        {
            if (!_isStorageInitialized) InitializeStorage();

            return _txSupport.ReadTxLogFromPartition(txRec);
        }

        public IRollback Commit(ITransactionContext tx)
        {
            // Set respective append offset.
            // Some serializers can skip null fields.
            var pd = tx.GetPartitionTx(PartitionID);
            var count = pd.NextRowID;
            foreach (var file in _columnStorage.AllOpenedFiles())
            {
                var column = _metadata.GetColumnById(file.ColumnID);
                var size = StorageSizeUtils.GetRecordSize(column, file.DataType);
                if (size > 0)
                {
                    pd.AppendOffset[file.FileID] = count*size;
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
                     || timestampType == EFieldType.DateTimeEpochMilliseconds
                ? DateUtils.DateTimeToUnixTimeStamp(value)
                : DateUtils.ToUnspecifiedDateTicks(value);
            
            return ColumnValueBinarySearch.LongBinarySerach(_timestampColumn, values, 0L, hi);
        }

        public IEnumerable<long> GetSymbolRows(string symbol, string value, 
            IReadTransactionContext tx)
        {
            if (!_isStorageInitialized) InitializeStorage();

            var symb = _symbols[symbol];
            var key = symb.CheckKeyQuick(value, tx);
            return symb.GetValues(key, tx);
        }

        public int GetSymbolKey(string symbol, string value, IReadTransactionContext tx)
        {
            if (!_isStorageInitialized) InitializeStorage();

            var symb = _symbols[symbol];
            var key = symb.CheckKeyQuick(value, tx);
            return key;
        }

        public IEnumerable<long> GetSymbolRows(string symbol, int valueKey, IReadTransactionContext tx)
        {
            if (!_isStorageInitialized) InitializeStorage();

            var symb = _symbols[symbol];
            return symb.GetValues(valueKey, tx);
        }

        public long GetSymbolRowCount(string symbol, string value, IReadTransactionContext tx)
        {
            if (!_isStorageInitialized) InitializeStorage();

            var symb = _symbols[symbol];
            var key = symb.CheckKeyQuick(value, tx);
            return symb.GetCount(key, tx);
        }

        private void InitializeStorage()
        {
            lock (_syncRoot)
            {
                if (!_isStorageInitialized)
                {
                    _columnStorage = new ColumnStorage(_metadata.Settings, StartDate,
                        _access, PartitionID, _memeorymMappedFileFactory);

                    ColumnSource[] columns = _metadata.GetPartitionColums(_columnStorage).ToArray();
                    _symbols = columns
                        .Where(c => c.Metadata.DataType == EFieldType.Symbol)
                        .Select(c => c.Column)
                        .Cast<ISymbolMapColumn>()
                        .ToDictionary(c => c.PropertyName, StringComparer.OrdinalIgnoreCase);

                    if (_metadata.TimestampFieldID.HasValue)
                    {
                        _timestampColumn =
                            (IFixedWidthColumn) columns[_metadata.TimestampFieldID.Value].Column;
                    }

                    _fieldSerializer = _metadata.GetSerializer(columns);
                    _txSupport = new FileTxSupport(PartitionID, _columnStorage, _metadata);
                   
                    Thread.MemoryBarrier();
                    _isStorageInitialized = true;
                }
            }
        }
    }
}