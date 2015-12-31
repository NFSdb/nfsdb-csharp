using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    internal interface IUnsafePartitionManager : IPartitionManager
    {
        IColumnStorage SymbolFileStorage { get; }
        IPartition GetPartition(int paritionID);
        IPartition[] GetOpenPartitions();
        void Recycle(TxState state);
        void DetachPartition(int partitionID);
        void ClearTxLog();
    }
}