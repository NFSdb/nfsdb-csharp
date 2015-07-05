using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IPartitionReader : IFileTxSupport
    {
        int GetOpenFileCount();
        long GetTotalMemoryMapped();
        long BinarySearchTimestamp(DateTime value, IReadTransactionContext tx);

        IEnumerable<long> GetSymbolRows(string symbol, string value,
            IReadTransactionContext tx);

        int GetSymbolKey(string symbol, string value, IReadTransactionContext tx);

        IEnumerable<long> GetSymbolRows(string symbol, int valueKey,
            IReadTransactionContext tx);

        long GetSymbolRowCount(string symbol, string value, IReadTransactionContext tx);

        object Read(long toLocalRowID, IReadContext readContext);

        int PartitionID { get; }

    }
}