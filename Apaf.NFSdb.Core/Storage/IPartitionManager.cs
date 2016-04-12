using System;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    internal interface IPartitionManager : IDisposable
    {
        EFileAccess Access { get; }

        ITransactionContext ReadTxLog();

        ITransactionContext ReadTxLog(int openPartitionTtlMs);

        void Commit(ITransactionContext transaction, int partitionTtl);

        IPartition GetAppendPartition(DateTime dateTime, ITransactionContext tx);

        event Action<long, long> OnCommited;

        IPartition CreateTempPartition(int partitionID, DateTime startDateTime, int lastVersion);

        void RemoveTempPartition(IPartition partition);

        void CommitTempPartition(IPartition tempPartition, PartitionTxData txData);
    }
}