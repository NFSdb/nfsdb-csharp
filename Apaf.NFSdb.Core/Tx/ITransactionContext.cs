using System.Linq;

namespace Apaf.NFSdb.Core.Tx
{
    public interface ITransactionContext : IReadTransactionContext
    {
        PartitionTxData AddPartition(int partitionID);
        long PrevTxAddress { get; set; }
        bool IsParitionUpdated(int partitionID, TransactionContext lastTransactionLog);
    }

    public class PartitionTxData
    {
        public bool IsPartitionUpdated;
        public long NextRowID;
        public long LastTimestamp;
        public long[] AppendOffset;
        public SymbolTxData[] SymbolData;

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