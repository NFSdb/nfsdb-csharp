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
    public class SymbolFilter<T> : IPartitionFilter
    {
        private readonly ColumnMetadata _column;
        private readonly T[] _values;

        public SymbolFilter(ColumnMetadata column, T value)
        {
            _column = column;
            _values = new [] {value};
        }

        public SymbolFilter(ColumnMetadata column, T[] values)
        {
            _column = column;
            _values = values;
        }

        public IEnumerable<long> Filter(IEnumerable<PartitionRowIDRange> partitions,
            IReadTransactionContext tx, ERowIDSortDirection sortDirection)
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