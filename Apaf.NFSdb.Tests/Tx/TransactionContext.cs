#region copyright
/*
 * Copyright (c) 2014. APAF http://apafltd.co.uk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Tests.Tx
{
    public class TransactionContext : ITransactionContext
    {
        private readonly int _columnCount;
        private readonly IPartition[] _partitions;
        private readonly ITxPartitionLock _partitionLock;
        private readonly ReadContext _readCatch = new ReadContext();
        private PartitionTxData _currentPartitionTx;
        private bool _isCommited;

        public TransactionContext(int columnCount, PartitionTxData[] partitionData, IPartition[] partitions, ITxPartitionLock partitionLock)
        {
            _columnCount = columnCount;
            _partitions = partitions;
            _partitionLock = partitionLock;
            PartitionTx = partitionData;
        }

        public TransactionContext(int columnCount)
        {
            _columnCount = columnCount;
        }

        public TransactionContext(TransactionContext copyFrom)
        {
            _columnCount = copyFrom._columnCount;
            if (copyFrom.PartitionTx != null)
            {
                PartitionTx = copyFrom.PartitionTx
                    .Select(DeepClone).ToArray();
            }
        }

        private static PartitionTxData DeepClone(PartitionTxData p)
        {
            var r = new PartitionTxData(p.AppendOffset.Length, p.PartitionID, p.StartDate, p.EndDate, new ReadContext())
            {
                LastTimestamp = p.LastTimestamp,
                NextRowID = p.NextRowID,
                IsPartitionUpdated = p.IsPartitionUpdated
            };

            for (int i = 0; i < p.AppendOffset.Length; i++)
            {
                r.AppendOffset[i] = p.AppendOffset[i];
                r.SymbolData[i] = p.SymbolData[i].DeepClone();
            }
            return r;
        }

        public IList<PartitionTxData> PartitionTx { get; private set; }

        public long GetRowCount(int partitionID)
        {
            return PartitionTx[partitionID].NextRowID;
        }

        public void AddRef(int partitionID)
        {
            throw new NotImplementedException();
        }

        public void RemoveRef(int paritiotnID)
        {
            throw new NotImplementedException();
        }

        public IList<IPartition> Partitions
        {
            get { return _partitions; }
        }

        public void SetCommited()
        {
            _isCommited = true;
        }

        public IList<int> PartitionIDs
        {
            get { return PartitionTx.Select(p => p.PartitionID).ToArray(); }
        }

        public ITxPartitionLock TxPartitions
        {
            get { return _partitionLock; }
        }

        public IPartitionReader Read(int partitionID)
        {
            return _partitions.Single(p => p.PartitionID == partitionID);
        }

        public IEnumerable<IPartitionReader> ReadPartitions { get; private set; }
        public IEnumerable<IPartitionReader> ReverseReadPartitions { get; private set; }
        public int PartitionCount { get; private set; }

        public void AddRefsAllPartitions()
        {
        }

        public void RemoveRefsAllPartitions()
        {
        }

        public PartitionTxData GetPartitionTx()
        {
            return _currentPartitionTx;
        }

        public PartitionTxData GetPartitionTx(int partitionID)
        {
            return PartitionTx[partitionID];
        }

        public PartitionTxData SetCurrentPartition(int partitionID)
        {
            return _currentPartitionTx = GetPartitionTx(partitionID);
        }

        public int PartitionTxCount { get { return PartitionTx.Count; } }

        public void AddPartition(IFileTxSupport parition)
        {
            throw new NotSupportedException();
        }

        public void AddPartition(PartitionTxData partitionData, int partitionID)
        {
            if (PartitionTx == null || partitionID >= PartitionTx.Count)
            {
                var oldParitions = PartitionTx;
                PartitionTx = new PartitionTxData[partitionID + 1];
                for (int i = 0; i < PartitionTx.Count; i++)
                {
                    if (oldParitions != null && i < oldParitions.Count)
                    {
                        PartitionTx[i] = DeepClone(oldParitions[i]);
                    }
                    else
                    {
                        PartitionTx[i] = partitionData;
                    }
                }
            }
        }

        public void AddPartition(IPartition parition)
        {
            throw new NotImplementedException();
        }

        public long PrevTxAddress { get; set; }

        public bool IsParitionUpdated(int partitionID, ITransactionContext lastTransactionLog)
        {
            var thisPd = PartitionTx[partitionID];
            var partitionTx = lastTransactionLog.GetPartitionTx(partitionID);

            if (partitionTx == null || PartitionTx.Count <= partitionID)
            {
                return true;
            }
            var lastPd = PartitionTx[partitionID];

            for (int i = 0; i < thisPd.AppendOffset.Length; i++)
            {
                if (thisPd.AppendOffset[i] != lastPd.AppendOffset[i]
                    || thisPd.SymbolData[i].KeyBlockOffset != lastPd.SymbolData[i].KeyBlockOffset
                    || thisPd.SymbolData[i].KeyBlockSize  != lastPd.SymbolData[i].KeyBlockSize)
                {
                    return true;
                }
            }

            return false;
        }

        public DateTime LastAppendTimestamp { get; set; }

        public IReadContext ReadCache
        {
            get { return _readCatch; }
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}