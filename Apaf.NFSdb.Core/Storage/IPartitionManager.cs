﻿using System;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IPartitionManager : IDisposable
    {
        EFileAccess Access { get; }

        ITransactionContext ReadTxLog();

        ITransactionContext ReadTxLog(int openPartitionTtlMs);

        void Commit(ITransactionContext transaction, int partitionTtl);

        IPartition GetAppendPartition(DateTime dateTime, ITransactionContext tx);
    }
}