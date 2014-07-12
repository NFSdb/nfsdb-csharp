//using Apaf.NFSdb.Core.Collections;
//using Apaf.NFSdb.Core.Storage;

//namespace Apaf.NFSdb.Core.Tx
//{
//    public class FixedTransactionContext : ITransactionContext
//    {
//        private readonly long[][] _appendOffset;
//        private readonly bool[][] _hasKeyBlockCreated;
//        private readonly long[][] _keyBlockSize;
//        private readonly long[][] _keyBlockOffset;
//        private readonly bool[] _partitionChanged;
//        private readonly long[] _lastTimestamp;
//        private readonly long[] _lastNextID;
        
//        private readonly ReadContext _readCatch = new ReadContext();

//        public FixedTransactionContext(ITransactionContext copyFrom)
//        {
//            _lastNextID = new ExpandableList<long>(copyFrom._lastNextID);
//            for (int i = 0; i < copyFrom._appendOffset.Count; i++)
//            {
//                _appendOffset[i] = new ExpandableList<long>(copyFrom._appendOffset[i]);
//            }
//            for (int i = 0; i < copyFrom._keyBlockSize.Count; i++)
//            {
//                _keyBlockSize[i] = new ExpandableList<long>(copyFrom._keyBlockSize[i]);
//            }
//            for (int i = 0; i < copyFrom._keyBlockOffset.Count; i++)
//            {
//                _keyBlockOffset[i] = new ExpandableList<long>(copyFrom._keyBlockOffset[i]);
//            }
//        }

//        public long GetAppendOffset(int partitionID, int fileID)
//        {
//            return _appendOffset[partitionID][fileID];
//        }
        
//        public void SetAppendOffset(int partitionID, int fileID, long value)
//        {
//            _appendOffset[partitionID][fileID] = value;
//            _partitionChanged[partitionID] = true;
//        }

//        public bool IsParitionUpdated(int partitoinID)
//        {
//            return _partitionChanged[partitoinID];
//        }

//        public void ResetChangedPartitions()
//        {
//            _partitionChanged.Clear();
//        }

//        public long GetNextRowID(int partitionID)
//        {
//            return _lastNextID[partitionID];
//        }

//        public void SetNextRowID(int partitionID, long lastRowID)
//        {
//            _lastNextID[partitionID] = lastRowID;
//        }

//        public bool GetKeyBlockCreated(int partitionID, int fileID)
//        {
//            return _hasKeyBlockCreated[partitionID][fileID];
//        }

//        public void SetKeyBlockCreated(int partitionID, int fileID, bool value)
//        {
//            _hasKeyBlockCreated[partitionID][fileID] = value;
//        }

//        public long GetKeyBlockSize(int partitionID, int fileID)
//        {
//            return _keyBlockSize[partitionID][fileID];
//        }

//        public long GetKeyBlockOffset(int partitionID, int fileID)
//        {
//            return _keyBlockOffset[partitionID][fileID];
//        }

//        public void SetKeyBlockSize(int partitionID, int fileID, long value)
//        {
//            _keyBlockSize[partitionID][fileID] = value;
//        }

//        public void SetKeyBlockOffset(int partitionID, int fileID, long value)
//        {
//            _keyBlockOffset[partitionID][fileID] = value;
//        }

//        public void SetLastTimestamp(int partitionID, long timestamp)
//        {
//            _lastTimestamp[partitionID] = timestamp;
//        }

//        public long GetLastTimestamp(int partitionID)
//        {
//            return _lastTimestamp[partitionID];
//        }

//        public long PrevTxAddress    { get; set; }

//        public IReadContext ReadCache
//        {
//            get { return _readCatch; }
//        }

//        public long GetRowCount(int partitionID)
//        {
//            return _lastNextID[partitionID];
//        }
//    }
//}