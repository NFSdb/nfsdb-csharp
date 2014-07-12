using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core
{
    public interface IPartitionCore
    {
        int PartitionID { get; }
        string DirectoryPath { get; }
        DateTime StartDate { get; }
        DateTime EndDate { get; }

        IColumnStorage Storage { get; }
        long BinarySearchTimestamp(DateTime value, IReadTransactionContext tx);
        
        IEnumerable<long> GetSymbolRows(string symbol, string value, 
            IReadTransactionContext tx);

        int GetSymbolKey(string symbol, string value, IReadTransactionContext tx);

        IEnumerable<long> GetSymbolRows(string symbol, int valueKey, 
            IReadTransactionContext tx);

        long GetSymbolRowCount(string symbol, string value,
            IReadTransactionContext tx);
    }
}