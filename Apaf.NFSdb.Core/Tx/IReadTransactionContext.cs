using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Tx
{
    public interface IReadTransactionContext
    {
        IReadContext ReadCache { get; }
        PartitionTxData[] PartitionTx { get; }
        long GetRowCount(int partitionID);
    }
}