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

using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Tx
{
    public interface ITransactionContext : IReadTransactionContext
    {
        void AddPartition(IFileTxSupport parition);
        void AddPartition(PartitionTxData partitionData, int partitionID);
        long PrevTxAddress { get; set; }
        bool IsParitionUpdated(int partitionID, ITransactionContext lastTransactionLog);
    }

    public class PartitionTxData
    {
        private PartitionTxData()
        {
        }

        public PartitionTxData(int columnCount)
        {
            AppendOffset = new long[columnCount];
            SymbolData = Enumerable.Range(0, columnCount)
                .Select(dd => new SymbolTxData()).ToArray();
        }

        public bool IsPartitionUpdated;
        public long NextRowID;
        public long LastTimestamp;
        public long[] AppendOffset;
        public IList<SymbolTxData> SymbolData;
        public bool IsAppended;

        public PartitionTxData DeepClone()
        {
            var c = new PartitionTxData
            {
                IsPartitionUpdated = IsPartitionUpdated,
                NextRowID = NextRowID,
                LastTimestamp = LastTimestamp,
                AppendOffset = AppendOffset.ToList().ToArray(),
                SymbolData = SymbolData.Select(
                    sd => sd.DeepClone()).ToArray()
            };
            return c;
        }
    }

    public class SymbolTxData
    {
        public SymbolTxData()
        {
        }

        public SymbolTxData(bool isKeyCreated, int blockSize, long blockOffset)
        {
            KeyBlockCreated = isKeyCreated;
            KeyBlockSize = blockSize;
            KeyBlockOffset = blockOffset;
        }

        public bool KeyBlockCreated;
        public int KeyBlockSize;
        public long KeyBlockOffset;

        public SymbolTxData DeepClone()
        {
            return new SymbolTxData(false, KeyBlockSize, KeyBlockOffset);
        }
    }
}