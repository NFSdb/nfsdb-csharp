#region copyright
/*
 * Copyright (c) 2014. APAF (Alex Pelagenko).
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
using System.Linq;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Tx
{
    public class TransactionContext : ITransactionContext
    {
        private readonly int _columnCount;
        private readonly ReadContext _readCatch = new ReadContext();

        public TransactionContext(int columnCount, PartitionTxData[] partitionData)
        {
            _columnCount = columnCount;
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
                    .Select(p => p.DeepClone()).ToArray();
            }
        }

        public PartitionTxData[] PartitionTx { get; private set; }

        public long GetRowCount(int partitionID)
        {
            return PartitionTx[partitionID].NextRowID;
        }

        public PartitionTxData AddPartition(int partitionID)
        {
            if (PartitionTx == null || partitionID >= PartitionTx.Length)
            {
                var oldParitions = PartitionTx;
                PartitionTx = new PartitionTxData[partitionID + 1];
                for (int i = 0; i < PartitionTx.Length; i++)
                {
                    if (oldParitions != null && i < oldParitions.Length)
                    {
                        PartitionTx[i] = oldParitions[i].DeepClone();
                    }
                    else
                    {
                        PartitionTx[i] = new PartitionTxData
                        {
                            AppendOffset = new long[_columnCount],
                            SymbolData = Enumerable.Range(0, _columnCount)
                                .Select(dd => new SymbolTxData()).ToArray()
                        };
                    }
                }
            }
            return PartitionTx[partitionID];
        }

        //public bool IsParitionUpdated(int partitoinID)
        //{
        //    if (PartitionTx != null)
        //    {
        //        var pd = PartitionTx[partitoinID];
        //        for (int i = 0; i < pd.AppendOffset.Length; i++)
        //        {
        //            if (pd.AppendOffset[i] != _appendOffset[partitoinID][i])
        //            {
        //                return true;
        //            }
        //        }
        //    }
        //    return false;
        //}
        
        public long PrevTxAddress    { get; set; }

        public bool IsParitionUpdated(int partitionID, TransactionContext lastTransactionLog)
        {
            var thisPd = PartitionTx[partitionID];
            if (lastTransactionLog.PartitionTx == null
                || lastTransactionLog.PartitionTx.Length <= partitionID)
            {
                return true;
            }
            var lastPd = lastTransactionLog.PartitionTx[partitionID];

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

        public IReadContext ReadCache
        {
            get { return _readCatch; }
        }
    }
}