using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IPartitionManagerCore : IDisposable
    {
        EFileAccess Access { get; }
        IEnumerable<IPartitionCore> GetOpenPartitions();
        IColumnStorage SymbolFileStorage { get; }

        void Truncate(ITransactionContext tx);

        ITransactionContext ReadTxLog();

        void Commit(ITransactionContext transaction);

        IPartitionReader Read(int paritionID);

    }
}