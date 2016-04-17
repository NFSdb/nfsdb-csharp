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
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Collections;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{

    public class LatestByFilter<T> : IPartitionFilter, ILatestBySymbolFilter
    {
        private readonly IJournalCore _journal;
        private readonly IColumnMetadata _column;
        private readonly IList<T> _keys;

        public LatestByFilter(IJournalCore journal, IColumnMetadata column, IList<T> keys)
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

        public IColumnMetadata Column
        {
            get { return _column; }
        }

        public long GetCardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            if (_keys == null)
            {
                return _journal.QueryStatistics.GetColumnDistinctCardinality(tx, _column);
            }
            return _keys.Count;
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
                var col = (ITypedColumn<T>) readPartition.ReadColumn(_column.ColumnID);
                for (long r = partition.High; r >= partition.Low; r--)
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
            List<long> latestRowIDs;
            if (_keys != null)
            {
                latestRowIDs = GetLatestByIndexedSymbolByKeys(partitions, tx);
            }
            else
            {
                latestRowIDs = GetAllLatestByIndexedSymbolByKeys(partitions, tx);
            }
            
           
            // RowID sort asc.
            latestRowIDs.Sort();
            int startIndex = latestRowIDs.BinarySearch(1L);
            if (startIndex < 0)
            {
                startIndex = ~startIndex;
            }

            var result = new ArraySlice<long>(latestRowIDs, startIndex, latestRowIDs.Count - startIndex,
                sort == ERowIDSortDirection.Asc);
            return result;
        }

        private List<long> GetAllLatestByIndexedSymbolByKeys(IEnumerable<PartitionRowIDRange> partitions, IReadTransactionContext tx)
        {
            var allKeys = new ObjIntHashMap();
            var latestRowIDs = new List<long>();

            foreach (var part in partitions)
            {
                var partition = tx.Read(part.PartitionID);
                var symbolColumn = (ISymbolMapColumn)partition.ReadColumn(Column.ColumnID);

                // Key mapping.
                var partitionTxData = tx.GetPartitionTx(part.PartitionID);
                var distinctCount = symbolColumn.GetDistinctCount(partitionTxData);

                for (int i = 0; i < distinctCount; i++)
                {
                    var key = i;
                    var symbolValue = symbolColumn.GetKeyValue(key, partitionTxData);
                    if (symbolValue == null)
                    {
                        key = MetadataConstants.NULL_SYMBOL_VALUE;
                    }
                    var existing = allKeys.Get(symbolValue);
                    if (existing == MetadataConstants.SYMBOL_NOT_FOUND_VALUE)
                    {
                        var rowIDs = partition.GetSymbolRowsByKey(_column.ColumnID, key, tx);

                        foreach (var rowID in rowIDs)
                        {
                            if (rowID >= part.Low && rowID <= part.High)
                            {
                                // Stop search the key.
                                latestRowIDs.Add(RowIDUtil.ToRowID(part.PartitionID, rowID));
                                allKeys.Put(symbolValue, 1);
                                break;
                            }
                        }
                    }
                }
            }

            return latestRowIDs;
        }

        private List<long> GetLatestByIndexedSymbolByKeys(IEnumerable<PartitionRowIDRange> partitions, IReadTransactionContext tx)
        {
            var latestRowIDs = new List<long>(_keys.Count);
            foreach (var part in partitions)
            {
                var partition = tx.Read(part.PartitionID);
                int[] keysMap = new int[_keys.Count];

                // Key mapping.
                for (int i = 0; i < _keys.Count; i++)
                {
                    keysMap[i] = partition.GetSymbolKey(_column.ColumnID, _keys[i], tx);
                }

                var allFound = true;
                for (int i = 0; i < _keys.Count; i++)
                {
                    if (latestRowIDs[i] == 0)
                    {
                        // Symbol D file key.
                        var key = keysMap[i];
                        if (key != MetadataConstants.SYMBOL_NOT_FOUND_VALUE)
                        {
                            var rowIDs = partition.GetSymbolRowsByKey(_column.ColumnID, key, tx);

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

            return latestRowIDs;
        }

        public override string ToString()
        {
            if (_keys == null)
            {
                return string.Format("Latest_By({0})", _column.PropertyName);
            }
            return string.Format("Latest_By({0} in ({1}))", _column.PropertyName,
                string.Join(",", _keys));
        }
    }
}