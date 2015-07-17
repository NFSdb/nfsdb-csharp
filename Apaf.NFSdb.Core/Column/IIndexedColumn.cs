using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public interface IIndexedColumn<in T> : IIndexedColumnCore
    {
        int CheckKeyQuick(T value, IReadTransactionContext tx);
    }
}