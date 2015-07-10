using System;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IPartitionManagerCore : IDisposable
    {
        EFileAccess Access { get; }

        ITransactionContext ReadTxLog();

        ITransactionContext ReadTxLog(int openPartitionTtlMs);

        void Commit(ITransactionContext transaction, int partitionTtl);
    }
}