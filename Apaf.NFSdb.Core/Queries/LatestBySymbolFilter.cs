using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class LatestBySymbolFilter : IPartitionFilter
    {
        private readonly IJournalCore _journal;
        private readonly string _latestBySymbol;
        private readonly string[] _keys;

        public LatestBySymbolFilter(IJournalCore journal, string latestBySymbol, string[] keys)
        {
            _journal = journal;
            _latestBySymbol = latestBySymbol;
            _keys = keys;
        }

        public IEnumerable<long> Filter(IEnumerable<PartitionRowIDRange> partitions, 
            IReadTransactionContext tx)
        {
            int keysCount;
            int[] keysMap = null;
            if (_keys == null)
            {
                keysCount = _journal.QueryStatistics
                    .GetSymbolCount(tx, _latestBySymbol);
            }
            else
            {
                keysCount = _keys.Length;
            }

            var foundKeys = new bool[keysCount];
            var latestRowIDs = new long[keysCount];
            foreach (var part in partitions)
            {
                // Key mapping.
                if (_keys != null && keysMap == null)
                {
                    keysMap = new int[_keys.Length];
                    for (int i = 0; i < _keys.Length; i++)
                    {
                        keysMap[i] = part.Partition.GetSymbolKey(
                            _latestBySymbol, _keys[i], tx);
                    }
                }

                var allFound = true;
                for (int i = 0; i < keysCount; i++)
                {
                    if (!foundKeys[i])
                    {
                        // Symbol D file key.
                        var key = keysMap == null ? i : keysMap[i];
                        if (key != MetadataConstants.SYMBOL_NOT_FOUND_VALUE)
                        {
                            var rowIDs =
                                part.Partition.GetSymbolRows(_latestBySymbol, key, tx);

                            foreach (var rowID in rowIDs)
                            {
                                if (rowID >= part.Low && rowID <= part.High)
                                {
                                    // Stop search the key.
                                    foundKeys[i] = true;
                                    latestRowIDs[i] =
                                        RowIDUtil.ToRowID(part.Partition.PartitionID, rowID);
                                    break;
                                }
                            }
                        }
                        else
                        {
                            // Stop search the invalid value.
                            foundKeys[i] = true;
                        }
                    }
                    allFound &= foundKeys[i];
                }

                // Early partition scan termination.
                if (allFound)
                {
                    break;
                }
            }

            // RowID sort desc.
            Array.Sort(latestRowIDs);
            for (int i = foundKeys.Length - 1; i >= 0; i--)
            {
                // Not found.
                // Rest is greater and returned.
                if (latestRowIDs[i] == 0) yield break;
                
                // Found.
                yield return latestRowIDs[i];
            }
        }
    }
}