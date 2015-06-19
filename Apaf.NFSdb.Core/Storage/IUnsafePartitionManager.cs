using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    internal interface IUnsafePartitionManager : IPartitionManagerCore
    {
        IColumnStorage SymbolFileStorage { get; }
        IPartitionCore GetPartition(int paritionID);
        IPartitionCore[] GetOpenPartitions();
        void Recycle(TxReusableState state);
        void DetachPartition(int partitionID);
        void ClearTxLog();
    }
}