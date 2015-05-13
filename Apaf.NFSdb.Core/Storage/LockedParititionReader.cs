using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{

    public class LockedParititionReader : ILockedParititionReader
    {
        private readonly IPartitionCore _partition;

        public LockedParititionReader(IPartitionCore partition)
        {
            _partition = partition;
        }

        public int GetOpenFileCount()
        {
            return _partition.GetOpenFileCount();
        }

        public long GetTotalMemoryMapped()
        {
            return _partition.GetTotalMemoryMapped();
        }

        public long BinarySearchTimestamp(DateTime value, IReadTransactionContext tx)
        {
            return _partition.BinarySearchTimestamp(value, tx);
        }

        public IEnumerable<long> GetSymbolRows(string symbol, string value, IReadTransactionContext tx)
        {
            return _partition.GetSymbolRows(symbol, value, tx);
        }

        public int GetSymbolKey(string symbol, string value, IReadTransactionContext tx)
        {
            return _partition.GetSymbolKey(symbol, value, tx);
        }

        public IEnumerable<long> GetSymbolRows(string symbol, int valueKey, IReadTransactionContext tx)
        {
            return _partition.GetSymbolRows(symbol, valueKey, tx);
        }

        public long GetSymbolRowCount(string symbol, string value, IReadTransactionContext tx)
        {
            return _partition.GetSymbolRowCount(symbol, value, tx);
        }

        public void Dispose()
        {
            _partition.RemoveReadRef();
        }
    }
}