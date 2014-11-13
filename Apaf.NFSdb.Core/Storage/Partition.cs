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
        private readonly IFieldSerializer _fieldSerializer;
        private readonly ColumnStorage _columnStorage;
        private readonly DateTime _endDate;
        private readonly FileTxSupport _txSupport;
        private readonly IFixedWidthColumn _timestampColumn;
        private readonly Dictionary<string, ISymbolMapColumn> _symbols;
        private readonly ColumnSource[] _columns;
        private readonly IJournalMetadata<T> _metadata;

        public Partition(IJournalMetadata<T> metadata,
            ICompositeFileFactory memeorymMappedFileFactory,
            EFileAccess access,
            DateTime startDate, int partitionID,
            string path)
        {
            _columnStorage = new ColumnStorage(metadata.Settings, startDate,
                access, partitionID, memeorymMappedFileFactory);

            _metadata = metadata;
            _columns = metadata.GetPartitionColums(_columnStorage).ToArray();
            _symbols = _columns
                .Where(c => c.Metadata.DataType == EFieldType.Symbol)
                .Select(c => c.Column)
                .Cast<ISymbolMapColumn>()
                .ToDictionary(c => c.PropertyName, StringComparer.OrdinalIgnoreCase);

            if (metadata.TimestampFieldID.HasValue)
            {
                _timestampColumn = 
                    (IFixedWidthColumn)_columns[metadata.TimestampFieldID.Value].Column;
            }

            _fieldSerializer = metadata.GetSerializer(_columns);
            StartDate = startDate;
            _endDate = PartitionManagerUtils.GetPartitionEndDate(
                startDate, metadata.Settings.PartitionType);
            
            PartitionID = partitionID;
            DirectoryPath = path;
            _txSupport = new FileTxSupport(partitionID, _columnStorage, metadata);
        }

        public IColumnStorage Storage { get { return _columnStorage; } }
        public DateTime StartDate { get; private set; }
        public DateTime EndDate { get { return _endDate; } }
        public int PartitionID { get; private set; }
        public string DirectoryPath { get; private set; }

        public T Read(long rowID, IReadContext readContext)
        {
            return (T) _fieldSerializer.Read(rowID, readContext);
        }

        public void Append(T item, ITransactionContext tx)
        {
            var pd = tx.PartitionTx[PartitionID];
            _fieldSerializer.Write(item, pd.NextRowID, tx);
            pd.NextRowID++;
        }

        public void Dispose()
        {
            _columnStorage.Dispose();
        }

        public void ReadTxLogFromPartition(ITransactionContext tx, TxRec txRec)
        {
            _txSupport.ReadTxLogFromPartition(tx, txRec);
        }

        public void Commit(ITransactionContext tx, ITransactionContext oldTxContext)
        {
            // Set respective append offset.
            // Some serializers can skip null fields.
            var pd = tx.PartitionTx[PartitionID];
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

            _txSupport.Commit(tx, oldTxContext);
        }

        public void SetTxRec(ITransactionContext tx, TxRec rec)
        {
            _txSupport.SetTxRec(tx, rec);
        }

        public long BinarySearchTimestamp(DateTime value, IReadTransactionContext tx)
        {
            if (_timestampColumn == null)
            {
                throw new NFSdbConfigurationException("timestampColumn is not configured for journal in "
                    + DirectoryPath);
            }

            var hi = tx.GetRowCount(PartitionID);
            var valuets = DateUtils.DateTimeToUnixTimeStamp(value);
            return ColumnValueBinarySearch.LongBinarySerach(_timestampColumn, valuets, 0L, hi);
        }

        public IEnumerable<long> GetSymbolRows(string symbol, string value, 
            IReadTransactionContext tx)
        {
            var symb = SymbolMapColumn(symbol);
            var key = symb.CheckKeyQuick(value, tx);
            return symb.GetValues(key, tx);
        }

        public int GetSymbolKey(string symbol, string value, IReadTransactionContext tx)
        {
            var symb = SymbolMapColumn(symbol);
            var key = symb.CheckKeyQuick(value, tx);
            return key;
        }

        public IEnumerable<long> GetSymbolRows(string symbol, int valueKey, IReadTransactionContext tx)
        {
            var symb = SymbolMapColumn(symbol);
            return symb.GetValues(valueKey, tx);
        }

        public long GetSymbolRowCount(string symbol, string value, IReadTransactionContext tx)
        {
            var symb = SymbolMapColumn(symbol);
            var key = symb.CheckKeyQuick(value, tx);
            return symb.GetCount(key, tx);
        }

        private ISymbolMapColumn SymbolMapColumn(string symbol)
        {
            var symb = _symbols[symbol];
            return symb;
        }
    }
}