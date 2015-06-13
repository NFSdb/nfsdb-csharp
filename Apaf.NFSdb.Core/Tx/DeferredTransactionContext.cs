﻿using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core.Tx
{
    public class DeferredTransactionContext : ITransactionContext
    {
        private const int RESERVED_PARTITION_COUNT = 10;
        private readonly IReadContext _readCache = new ReadContext();
        private readonly ITxPartitionLock _parititionLock;
        private readonly IPartitionManagerCore _paritionManager;
        private readonly TxReusableState _reusableState;
        private readonly IList<int> _paritionIDs;

        private PartitionTxData _currentParitionTx;
        private TxRec _txRec;
        private PartitionTxData[] _txData;

        public DeferredTransactionContext(TxReusableState reusableState, IPartitionManagerCore paritionManager, TxRec txRec)
        {
            _reusableState = reusableState;
            _parititionLock = _reusableState.PartitionLock;
            _paritionManager = paritionManager;

            _paritionIDs = _reusableState.PartitionIDs;
            _txRec = txRec;
            _txData = new PartitionTxData[_paritionIDs.Count + 1 + RESERVED_PARTITION_COUNT];
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

        public IList<int> PartitionIDs
        {
            get { return _paritionIDs; }
        }

        public ITxPartitionLock TxPartitions { get { return _parititionLock; } }

        public IPartitionReader Read(int partitionID)
        {
            return _paritionManager.Read(partitionID);
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
                data = _paritionManager.ReadTx(partitionID).ReadTxLogFromPartition(
                    partitionID == _paritionIDs[_paritionIDs.Count - 1] ? _txRec : null);
                _txData[partitionID] = data;
            }
            return data;
        }

        public PartitionTxData SetCurrentPartition(int partitionID)
        {
            var data = GetPartitionTx(partitionID);
            return _currentParitionTx = data;
        }

        public int PartitionTxCount { get { return _paritionIDs.Count; } }

        public void AddPartition(int paritionID)
        {
            _paritionIDs.Add(paritionID);
            if (_txData.Length < _paritionIDs.Count)
            {
                var oldTx = _txData;
                _txData = new PartitionTxData[_paritionIDs.Count + RESERVED_PARTITION_COUNT];
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
            _parititionLock.Free();
            _reusableState.PartitionDataStorage = _txData;
            _paritionManager.Recycle(_reusableState);
        }
    }
}