using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    internal interface IUnsafePartitionManager : IPartitionManager
    {
        IColumnStorage SymbolFileStorage { get; }
        IPartition[] GetOpenPartitions();
        void Recycle(TxState state);
    }
}