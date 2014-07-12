//using Apaf.NFSdb.Core.Collections;
//using Apaf.NFSdb.Core.Storage;

//namespace Apaf.NFSdb.Core.Tx
//{
//    public class ReadTransactionContext : IReadTransactionContext
//    {
//        private readonly ExpandableList<long> _rowCounts; 

//        public ReadTransactionContext()
//        {
//            ReadCache = new ReadContext();
//            _rowCounts = new ExpandableList<long>();
//        }

//        protected ReadTransactionContext(ReadTransactionContext lastTransactionLog)
//        {
//            _rowCounts = new ExpandableList<long>(lastTransactionLog._rowCounts);
//        }

//        public IReadContext ReadCache { get; private set; }

//        public long GetRowCount(int partitionID)
//        {
//            return _rowCounts[partitionID];
//        }

//        public void SetRowCount(int partitionID, long count)
//        {
//            _rowCounts[partitionID] = count;
//        }
//    }
//}