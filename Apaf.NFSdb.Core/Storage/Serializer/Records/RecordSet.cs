using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage.Serializer.Records
{
    public class RecordSet : IRecordSet
    {
        private readonly IEnumerable<long> _rowIDs;
        private readonly IReadTransactionContext _tx;
        private readonly IJournalMetadata _metadata;
        private readonly int _bitSetColIndex = -1;
        private int[] _columnMaps;
        IPartitionReader _lastPartitionReader;
        int _lastPartitionID = -1;

        public RecordSet(IEnumerable<long> rowIDs, 
            IReadTransactionContext tx, 
            IJournalMetadata metadata)
        {
            _rowIDs = rowIDs;
            _tx = tx;
            _metadata = metadata;
            var bitsetColumn = _metadata.Columns.FirstOrDefault(c => c.FieldType == EFieldType.BitSet);
            if (bitsetColumn != null)
            {
                _bitSetColIndex = bitsetColumn.FieldID;
            }
        }

        public IRecordSet Map(IList<string> columnNames)
        {
            _columnMaps = new int[columnNames.Count];
            for (int i = 0; i < columnNames.Count; i++)
            {
                _columnMaps[i] = _metadata.GetColumnID(columnNames[i]);
            }
            return this;
        }

        public T? GetNullable<T>(long rowId, int columnIndex) where T : struct
        {
            int partitionID = RowIDUtil.ToPartitionIndex(rowId);
            long localRowID = RowIDUtil.ToLocalRowID(rowId);
            if (_columnMaps != null) columnIndex = _columnMaps[columnIndex];
            var nullable = _metadata.GetColumnById(columnIndex).Nullable;
            var nullIndex = _metadata.GetColumnById(columnIndex).NullIndex;

            if (nullable && _bitSetColIndex != -1)
            {
                if (partitionID == _lastPartitionID)
                {
                    byte[] bitset = ((IBitsetColumn)_lastPartitionReader.ReadColumn(_bitSetColIndex))
                        .GetValue(localRowID, _tx.ReadCache);
                    
                    if (new ByteArray(bitset).IsSet(nullIndex))
                    {
                        return default(T?);
                    }
                }
                else if (UpdatePartitionGetIsNull(partitionID, localRowID, nullIndex))
                {
                    return default(T?);
                }
            }
            
            if (partitionID == _lastPartitionID)
            {
                return ((ITypedColumn<T>)_lastPartitionReader.ReadColumn(columnIndex)).Get(localRowID, _tx.ReadCache);
            }

            return UpdatePartition<T>(partitionID, localRowID, columnIndex);
        }

        public bool IsNull(long rowId, int columnIndex)
        {
            int partitionID = RowIDUtil.ToPartitionIndex(rowId);
            long localRowID = RowIDUtil.ToLocalRowID(rowId);
            if (_columnMaps != null) columnIndex = _columnMaps[columnIndex];
            var nullable = _metadata.GetColumnById(columnIndex).Nullable;
            var nullIndex = _metadata.GetColumnById(columnIndex).NullIndex;

            if (nullable && _bitSetColIndex != -1)
            {
                if (partitionID == _lastPartitionID)
                {
                    byte[] bitset = ((IBitsetColumn) _lastPartitionReader.ReadColumn(_bitSetColIndex))
                        .GetValue(localRowID, _tx.ReadCache);
                    return new ByteArray(bitset).IsSet(nullIndex);
                }
                return UpdatePartitionGetIsNull(partitionID, localRowID, nullIndex);
            }
            return false;
        }

        public T Get<T>(long rowId, int columnIndex)
        {
            int partitionID = RowIDUtil.ToPartitionIndex(rowId);
            long localRowID = RowIDUtil.ToLocalRowID(rowId);
            if (_columnMaps != null) columnIndex = _columnMaps[columnIndex];

            if (partitionID == _lastPartitionID)
            {
                return ((ITypedColumn<T>)_lastPartitionReader.ReadColumn(columnIndex)).Get(localRowID, _tx.ReadCache);
            }

            return UpdatePartition<T>(partitionID, localRowID, columnIndex);
        }

        private T UpdatePartition<T>(int partitionID, long localRowID, int columnIndex)
        {
            _lastPartitionReader = _tx.Read(partitionID);
            _lastPartitionID = partitionID;
            return ((ITypedColumn<T>)_lastPartitionReader.ReadColumn(columnIndex)).Get(localRowID, _tx.ReadCache);
        }

        private bool UpdatePartitionGetIsNull(int partitionID, long localRowID, int nullIndex)
        {
            _lastPartitionReader = _tx.Read(partitionID);
            _lastPartitionID = partitionID;

            byte[] bitset = ((IBitsetColumn) _lastPartitionReader.ReadColumn(_bitSetColIndex))
                .GetValue(localRowID, _tx.ReadCache);
            return new ByteArray(bitset).IsSet(nullIndex);
        }

        public IEnumerable<long> RecordIDs()
        {
            return _rowIDs;
        }
    }
}