using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IPartitionManagerCore : IDisposable
    {
        EFileAccess Access { get; }
        IEnumerable<IPartitionCore> Partitions { get; }
        IColumnStorage SymbolFileStorage { get; }
        ITxLog TransactionLog { get; }

        IPartitionCore GetCorePartitionByID(int partitionID);
        ITransactionContext ReadTxLog();
        void Commit(ITransactionContext transaction); 
    }
}