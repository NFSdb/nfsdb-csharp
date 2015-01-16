using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Tx
{
    public class DeferredTransactionContext : ITransactionContext
    {
        private readonly List<IFileTxSupport> _partitions;
        private TxRec _txRec;
        private PartitionTxData[] _txData;
        private const int RESERVED_PARTITION_COUNT = 10;

        public DeferredTransactionContext(IFileTxSupport symbols, 
            IEnumerable<IFileTxSupport> partitions, TxRec txRec)
        {
            _partitions = new List<IFileTxSupport> { symbols };
            _partitions.AddRange(partitions);
            _txRec = txRec;
            _txData = new PartitionTxData[_partitions.Count + 1 + RESERVED_PARTITION_COUNT];
        }

        private readonly IReadContext _readCache = new ReadContext();
        private PartitionTxData _currentParitionTx;

        public IReadContext ReadCache
        {
            get { return _readCache; }
        }

        public long GetRowCount(int partitionID)
        {
            return GetPartitionTx(partitionID).NextRowID;
        }

        public PartitionTxData GetPartitionTx()
        {
            return _currentParitionTx;
        }

        public PartitionTxData GetPartitionTx(int partitionID)
        {
            var data = _txData[partitionID];
            if (data == null)
            {
                data = _partitions[partitionID].ReadTxLogFromPartition(
                    partitionID == _partitions.Count - 1 ? _txRec : null);
                _txData[partitionID] = data;
            }
            return data;
        }

        public void SetCurrentPartition(int partitionID)
        {
            _currentParitionTx = GetPartitionTx(partitionID);
        }

        public int PartitionTxCount { get { return _partitions.Count; } }

        public void AddPartition(IFileTxSupport parition)
        {
            _partitions.Add(parition);
            if (_txData.Length < _partitions.Count)
            {
                var oldTx = _txData;
                _txData = new PartitionTxData[_partitions.Count + RESERVED_PARTITION_COUNT];
                Array.Copy(oldTx, _txData, oldTx.Length);
            }
            _txRec = null;
        }

        public void AddPartition(PartitionTxData partitionData, int partitionID)
        {
            throw new NotSupportedException();
        }

        public long PrevTxAddress { get; set; }

        public bool IsParitionUpdated(int partitionID, ITransactionContext lastTransactionLog)
        {
            var data = _txData[partitionID];
            return data != null && data.IsAppended;
        }
    }
}