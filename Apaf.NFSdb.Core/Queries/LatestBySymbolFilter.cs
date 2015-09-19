#region copyright
/*
 * Copyright (c) 2014. APAF http://apafltd.co.uk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion
using System;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Collections;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class LatestBySymbolFilter<T> : IPartitionFilter
    {
        private readonly IJournalCore _journal;
        private readonly ColumnMetadata _column;
        private readonly T[] _keys;

        public LatestBySymbolFilter(IJournalCore journal, ColumnMetadata column, T[] keys)
        {
            _journal = journal;
            _column = column;
            _keys = keys;
        }

        public IEnumerable<long> Filter(IEnumerable<PartitionRowIDRange> partitions,
            IReadTransactionContext tx, ERowIDSortDirection sort)
        {
            if (_column.Indexed)
            {
                return GetLatestByIndexedSymbol(partitions, tx, sort);
            }
            return GetLatestByNonIndexedField(partitions, tx, sort);
        }

        public string Column
        {
            get { return _column.PropertyName; }
        }

        public long GetCardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            if (_keys == null)
            {
                return _journal.QueryStatistics.GetColumnDistinctCardinality(tx, _column);
            }
            return _keys.Length;
        }

        private IEnumerable<long> GetLatestByNonIndexedField(IEnumerable<PartitionRowIDRange> partitions,
            IReadTransactionContext tx, ERowIDSortDirection sort)
        {
            if (sort == ERowIDSortDirection.Desc)
            {
                return GetLatestFromDescending(partitions, tx);
            }
            return GetLatestFromDescending(partitions.Reverse(), tx).Reverse();
        }

        private IEnumerable<long> GetLatestFromDescending(IEnumerable<PartitionRowIDRange> partitions, IReadTransactionContext tx)
        {
            var latest = new HashSet<T>();
            HashSet<T> contains = _keys != null ? new HashSet<T>(_keys) : null;
            foreach (var partition in partitions)
            {
                var readPartition = tx.Read(partition.PartitionID);
                var col = (ITypedColumn<T>) readPartition.ReadColumn(_column.FieldID);
                for (long r = partition.High; r >= partition.Low; r++)
                {
                    var val = col.Get(r, tx.ReadCache);
                    if ((contains == null || contains.Contains(val)) && !latest.Contains(val))
                    {
                        latest.Add(val);
                        yield return RowIDUtil.ToRowID(partition.PartitionID, r);
                    }
                }
            }
        }

        private IEnumerable<long> GetLatestByIndexedSymbol(IEnumerable<PartitionRowIDRange> partitions,
            IReadTransactionContext tx, ERowIDSortDirection sort)
        {
            int keysCount;
            int[] keysMap = null;
            if (_keys == null)
            {
                keysCount = _journal.QueryStatistics.GetSymbolCount(tx, _column);
            }
            else
            {
                keysCount = _keys.Length;
            }

            // Todo: use tx.ReadContext
            var latestRowIDs = new long[keysCount];
            foreach (var part in partitions)
            {
                var partition = tx.Read(part.PartitionID);
                // Key mapping.
                if (_keys != null && keysMap == null)
                {
                    keysMap = new int[_keys.Length];
                    for (int i = 0; i < _keys.Length; i++)
                    {
                        keysMap[i] = partition.GetSymbolKey(_column.FieldID, _keys[i], tx);
                    }
                }

                var allFound = true;
                for (int i = 0; i < keysCount; i++)
                {
                    if (latestRowIDs[i] == 0)
                    {
                        // Symbol D file key.
                        var key = keysMap == null ? i : keysMap[i];
                        if (key != MetadataConstants.SYMBOL_NOT_FOUND_VALUE)
                        {
                            var rowIDs = partition.GetSymbolRowsByKey(_column.FieldID, key, tx);

                            foreach (var rowID in rowIDs)
                            {
                                if (rowID >= part.Low && rowID <= part.High)
                                {
                                    // Stop search the key.
                                    latestRowIDs[i] = RowIDUtil.ToRowID(part.PartitionID, rowID);
                                    break;
                                }
                            }
                        }
                        else
                        {
                            // Stop search the invalid value.
                            latestRowIDs[i] = MetadataConstants.SYMBOL_NOT_FOUND_VALUE;
                        }
                    }
                    allFound &= latestRowIDs[i] != 0;
                }

                // Early partition scan termination.
                if (allFound)
                {
                    break;
                }
            }

            // RowID sort asc.
            Array.Sort(latestRowIDs);
            int startIndex = Array.BinarySearch(latestRowIDs, 1L);
            if (startIndex < 0)
            {
                startIndex = ~startIndex;
            }

            var result = new ArraySlice<long>(latestRowIDs, startIndex, latestRowIDs.Length - startIndex,
                sort == ERowIDSortDirection.Asc);
            return result;
        }
    }
}