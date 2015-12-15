using System.Collections.Generic;
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

        public IEnumerable<long> RecordIDs()
        {
            return _rowIDs;
        }
    }
}