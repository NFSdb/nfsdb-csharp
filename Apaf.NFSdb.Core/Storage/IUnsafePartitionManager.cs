using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    internal interface IUnsafePartitionManager : IPartitionManager
    {
        IColumnStorage SymbolFileStorage { get; }
        IPartitionCore GetPartition(int paritionID);
        IPartitionCore[] GetOpenPartitions();
        void Recycle(TxState state);
        void DetachPartition(int partitionID);
        void ClearTxLog();
    }
}