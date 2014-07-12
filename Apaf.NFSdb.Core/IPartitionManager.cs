using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core
{
    public interface IPartitionManager<T> : IDisposable
    {
        EFileAccess Access { get; }
        IEnumerable<IPartition<T>> Partitions { get; }
        IColumnStorage SymbolFileStorage { get; }
        ITxLog TransactionLog { get; }

        IPartition<T> GetPartitionByID(int partitionID);
        ITransactionContext ReadTxLog();
        IPartition<T> GetAppendPartition(DateTime dateTime, ITransactionContext tx);
        void Commit(ITransactionContext transaction);
    }
}