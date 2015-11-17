using System.Collections.Generic;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Records
{
    public class RecordSet : IRecordSet
    {
        private readonly IEnumerable<long> _rowIDs;
        private readonly IReadTransactionContext _tx;
        private readonly IJournalMetadataCore _metadata;
        private int[] _columnMaps;
        IPartitionReader _lastPartitionReader;
        int _lastPartitionID = -1;

        public RecordSet(IEnumerable<long> rowIDs, 
            IReadTransactionContext tx, 
            IJournalMetadataCore metadata)
        {
            _rowIDs = rowIDs;
            _tx = tx;
            _metadata = metadata;
        }

        public void Map(IList<string> columnNames)
        {
            throw new System.NotImplementedException();
        }

        public T Get<T>(long rowId, int columnIndex)
        {
            int partitionID = RowIDUtil.ToPartitionIndex(rowId);
            long localRowID = RowIDUtil.ToLocalRowID(rowId);
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