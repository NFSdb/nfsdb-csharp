using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IPartitionReader : IFileTxSupport
    {
        int GetOpenFileCount();
        
        long GetTotalMemoryMapped();

        long BinarySearchTimestamp(DateTime value, IReadTransactionContext tx);

        IEnumerable<long> GetSymbolRows<T>(int fieldID, T value, IReadTransactionContext tx);

        int GetSymbolKey<T>(int fieldID, T value, IReadTransactionContext tx);

        IEnumerable<long> GetSymbolRowsByKey(int fieldID, int valueKey, IReadTransactionContext tx);

        long GetSymbolRowCount<T>(int fieldID, T value, IReadTransactionContext tx);

        T Read<T>(long toLocalRowID, ReadContext readContext);

        IColumn ReadColumn(int columnID);

        int PartitionID { get; }
    }
}