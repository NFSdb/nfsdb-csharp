using System;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core
{
    public interface IPartition<T> : IPartitionCore, IDisposable, IFileTxSupport
    {
        T Read(long rowID, IReadContext readContext);
        void Append(T item, ITransactionContext tx);
    }
}