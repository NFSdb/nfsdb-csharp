using System;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Collections;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core.Tx
{
    public class DeferredTransactionContext : ITransactionContext
    {
        private const int RESERVED_PARTITION_COUNT = 10;
        private readonly IReadContext _readCache = new ReadContext();
        private readonly IUnsafePartitionManager _paritionManager;
        private readonly TxState _state;
        private readonly IFileTxSupport _symbolTxSupport;
        private readonly List<IPartitionCore> _paritions;
        private readonly List<bool> _locks;

        private PartitionTxData _currentParitionTx;
        private TxRec _txRec;
        private readonly int _partitionTtlMs;
        private PartitionTxData[] _txData;
        private readonly int _lastPartitionID;

        internal DeferredTransactionContext(TxState state, 
            IFileTxSupport symbolTxSupport, 
            IUnsafePartitionManager paritionManager, 
            TxRec txRec,
            int partitionTtlMs)
        {
            _state = state;
            _symbolTxSupport = symbolTxSupport;
            _paritionManager = paritionManager;

            _paritions = _state.Partitions;
            _locks = _state.Locks;

            _locks.Clear();
            _locks.Capacity = _paritions.Count;

            var lastPartition = _paritions.LastNotNull();
            _lastPartitionID = lastPartition != null ? lastPartition.PartitionID : -1;

            _txRec = txRec;
            _partitionTtlMs = partitionTtlMs;
            _txData = new PartitionTxData[_paritions.Count + 1 + RESERVED_PARTITION_COUNT];
            LastAppendTimestamp = txRec != null ? DateUtils.UnixTimestampToDateTime(txRec.LastPartitionTimestamp) : DateTime.MinValue;
        }

        public IReadContext ReadCache
        {
            get { return _readCache; }
        }

        public long GetRowCount(int partitionID)
        {
            return GetPartitionTx(partitionID).NextRowID;
        }

        public void AddRef(int partitionID)
        {
            if (_locks.Count <= partitionID || !_locks[partitionID])
            {
                var p = _paritions[partitionID];
                if (p != null)
                {
                    p.AddRef();
                    _locks.SetToIndex(partitionID, true);
                }
            }
        }

        public void RemoveRef(int partitionID)
        {
            if (_locks.Count > partitionID && _locks[partitionID])
            {
                var p = _paritions[partitionID];
                if (p != null)
                {
                    p.RemoveRef(_partitionTtlMs);
                    _locks.SetToIndex(partitionID, true);
                }
            }
        }

        public IList<IPartitionCore> Partitions
        {
            get { return _paritions; }
        }

        public void AddRefsAllPartitions()
        {
            for (int i = 0; i < _paritions.Count; i++)
            {
                AddRef(i);
            }
        }

        public void RemoveRefsAllPartitions()
        {
            for (int i = 0; i < _paritions.Count; i++)
            {
                RemoveRef(i);
            }
        }

        public IPartitionReader Read(int partitionID)
        {
            return _paritions[partitionID];
        }

        public IEnumerable<IPartitionReader> ReadPartitions
        {
            get { return _paritions.Where(p => p != null); }
        }

        public IEnumerable<IPartitionReader> ReverseReadPartitions
        {
            get { return Enumerable.Reverse(_paritions).Where(p => p != null); }
        }

        public int PartitionCount
        {
            get { return _paritions.Count; }
        }

        public PartitionTxData GetPartitionTx()
        {
            return _currentParitionTx;
        }

        public PartitionTxData GetPartitionTx(int partitionID)
        {
            var data = _txData[partitionID];
            if (data != null)
            {
                return data;
            }
            return GetPartitionTx0(partitionID);
        }

        private PartitionTxData GetPartitionTx0(int partitionID)
        {
            IFileTxSupport txSpt = partitionID == 0 ? _symbolTxSupport : _paritions[partitionID];
            var data = txSpt.ReadTxLogFromPartition(partitionID == _lastPartitionID ? _txRec : null);
            _txData[partitionID] = data;
            return data;
        }

        public PartitionTxData SetCurrentPartition(int partitionID)
        {
            var data = GetPartitionTx(partitionID);
            return _currentParitionTx = data;
        }

        public void AddPartition(IPartitionCore parition)
        {
            _paritions.Add(parition);

            while (_paritions.Count <= parition.PartitionID)
            {
                _paritions.Add(null);
            }
            _paritions[parition.PartitionID] = parition;

            if (_txData.Length < _paritions.Count)
            {
                var oldTx = _txData;
                _txData = new PartitionTxData[_paritions.Count + RESERVED_PARTITION_COUNT];
                Array.Copy(oldTx, _txData, oldTx.Length);
            }

            _txRec = null;
        }
        
        public long PrevTxAddress { get; set; }
        public DateTime LastAppendTimestamp { get; set; }

        public bool IsParitionUpdated(int partitionID, ITransactionContext lastTransactionLog)
        {
            var data = _txData[partitionID];
            return data != null && data.IsAppended;
        }

        public void Dispose()
        {
            RemoveRefsAllPartitions();
            _state.PartitionDataStorage = _txData;
            _paritionManager.Recycle(_state);
        }
    }
}