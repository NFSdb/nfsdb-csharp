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
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class SymbolFilter<T> : IPartitionFilter, IColumnFilter
    {
        private readonly ColumnMetadata _column;
        private readonly T _value;
        private readonly T[] _values;
        private ITypedColumn<T> _columnReader;
        private int _columnPartition = int.MinValue;
        private HashSet<T> _valuesHash;

        public SymbolFilter(ColumnMetadata column, T value)
        {
            _column = column;
            _value = value;
        }

        public SymbolFilter(ColumnMetadata column, T[] values)
        {
            if (values == null) throw new ArgumentNullException("values");

            _column = column;
            if (values.Length == 1)
            {
                _value = _values[0];
            }
            else
            {
                _values = values;
            }
        }

        public string Column
        {
            get { return _column.PropertyName; }
        }

        public T[] FilterValues
        {
            get
            {
                if (_values != null)
                {
                    return _values;
                }
                return new[] {_value};
            }
        }

        private bool IsMatch(T value)
        {
            if (_values == null)
            {
                return Equals(value, _value);
            }

            if (_valuesHash == null)
            {
                _valuesHash = new HashSet<T>(_values);
            }
            return _valuesHash.Contains(value);
        }

        public bool IsMatch(IPartitionReader partition, IReadContext readCache, long localRowID)
        {
            if (_columnPartition != partition.PartitionID)
            {
                _columnPartition = partition.PartitionID;
                _columnReader = (ITypedColumn<T>)partition.ReadColumn(_column.FieldID);
            }

            var value = _columnReader.Get(localRowID, readCache);
            return IsMatch(value);
        }

        public long GetCardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            return journal.QueryStatistics.GetCardinalityByColumnValue(tx, _column, _values ?? new[] {_value});
        }

        public IEnumerable<long> Filter(IEnumerable<PartitionRowIDRange> partitions,
            IReadTransactionContext tx, ERowIDSortDirection sortDirection)
        {
            if (_values != null)
            {
                var items = new IEnumerable<long>[_values.Length];
                return partitions.SelectMany(part =>
                {
                    var partition = tx.Read(part.PartitionID);
                    for (int v = 0; v < _values.Length; v++)
                    {
                        var symbolValue = _values[v];
                        var rowIDs = TakeFromTo(part, partition.GetSymbolRows(_column.FieldID, symbolValue, tx));
                        if (sortDirection == ERowIDSortDirection.Asc)
                        {
                            // Todo: use tx.ReadContext and reuse buffers.
                            rowIDs = rowIDs.Reverse();
                        }
                        items[v] = rowIDs;
                    }

                    if (sortDirection == ERowIDSortDirection.None)
                    {
                        return items.SelectMany(i => i);
                    }
                    return MergeSorted(items, sortDirection);
                });
            }
            else
            {
                return partitions.SelectMany(part =>
                {
                    var partition = tx.Read(part.PartitionID);
                    var rowIDs = TakeFromTo(part, partition.GetSymbolRows(_column.FieldID, _value, tx));
                    if (sortDirection == ERowIDSortDirection.Asc)
                    {
                        rowIDs = rowIDs.Reverse();
                    }
                    return rowIDs;
                });
            }
        }

        private IEnumerable<long> TakeFromTo(PartitionRowIDRange part, IEnumerable<long> rowIds)
        {
            foreach (var rowId in rowIds)
            {
                if (rowId < part.Low)
                    yield break;

                if (rowId <= part.High)
                    yield return RowIDUtil.ToRowID(part.PartitionID, rowId);
            }
        }

        private IEnumerable<long> MergeSorted(IEnumerable<long>[] items, ERowIDSortDirection sortDirection)
        {
            if (items.Length == 1) return items[0];
            return GetMergeSorted(items, sortDirection);
        }

        private IEnumerable<long> GetMergeSorted(IEnumerable<IEnumerable<long>> items, ERowIDSortDirection sortDirection)
        {
            var ens = items.Select(i => i.GetEnumerator()).Where(e => e.MoveNext()).ToArray();

            // Trivial solutions.
            if (ens.Length == 0)
            {
                yield break;
            }

            if (ens.Length == 1)
            {
                do
                {
                    yield return ens[0].Current;
                } while (ens[0].MoveNext());
                yield break;
            }
            
            // Priority Queue.
            var pq = new EnumerablePriorityQueue(ens.Length, sortDirection == ERowIDSortDirection.Asc);
            for (int i = 0; i < ens.Length; i++)
            {
                pq.Enqueue(ens[i]);
            }

            while (pq.Count > 0)
            {
                var e = pq.Dequeue();
                yield return e.Current;
                if (e.MoveNext())
                {
                    pq.Enqueue(e);
                }
            }
        }

        private class EnumerablePriorityQueue : PriorityQueue<IEnumerator<long>>
        {
            private readonly bool _asc;

            public EnumerablePriorityQueue(int capcity, bool asc) : base(capcity)
            {
                _asc = asc;
            }

            protected override int Compare(IEnumerator<long> i1, IEnumerator<long> i2)
            {
                if (_asc && i1.Current > i2.Current && !_asc) return 1;
                if (!_asc && i1.Current < i2.Current) return 1;
                return -1;
            }
        }
    }
}