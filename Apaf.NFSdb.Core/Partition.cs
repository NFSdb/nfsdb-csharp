﻿using System;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core
{
    public class Partition<T> : IPartition<T>
    {
        private readonly IFieldSerializer _fieldSerializer;
        private readonly ColumnStorage _columnStorage;
        private readonly DateTime _endDate;
        private readonly FileTxSupport _txSupport;
        private readonly IColumn[] _columns;
        private readonly IFixedWidthColumn _timestampColumn;

        public Partition(IJournalMetadata<T> metadata,
            ICompositeFileFactory memeorymMappedFileFactory,
            EFileAccess access,
            DateTime startDate, int partitionID,
            string path)
        {
            _columnStorage = new ColumnStorage(metadata.Settings, startDate,
                access, partitionID, memeorymMappedFileFactory);
            
            _columns = metadata.GetPartitionColums(_columnStorage).ToArray();
            if (metadata.TimestampFieldID.HasValue)
            {
                _timestampColumn = 
                    (IFixedWidthColumn)_columns[metadata.TimestampFieldID.Value];
            }

            _fieldSerializer = new ThriftObjectSerializer(typeof(T), _columns);
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
            var symb = (ISymbolMapColumn)_columns.First(c =>
                c.PropertyName.Equals(symbol, StringComparison.OrdinalIgnoreCase));
            return symb;
        }
    }
}