using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Queries.Records
{
    public struct Record : IRecord
    {
        private readonly TransactionParition _parition;
        private readonly long _rowId;

        public Record(TransactionParition parition, long rowId)
        {
            _parition = parition;
            _rowId = rowId;
        }

        public T Get<T>(int columnIndex)
        {
            return ((ITypedColumn<T>) _parition.Partition.ReadColumn(columnIndex))
                .Get(_rowId, _parition.ReadContext);
        }

        public T Get<T>(string name)
        {
            int colIndex = _parition.GetColumnID(name);
            return Get<T>(colIndex);
        }
    }
}