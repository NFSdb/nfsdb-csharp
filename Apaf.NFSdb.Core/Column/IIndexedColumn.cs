using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public interface IIndexedColumn<T> : IIndexedColumnCore
    {
        int CheckKeyQuick(T value, PartitionTxData tx);
        int GetDistinctCount(PartitionTxData tx);
        T GetKeyValue(int key, PartitionTxData rc);
    }
}