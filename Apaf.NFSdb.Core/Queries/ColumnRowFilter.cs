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
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public abstract class ColumnFilterBase<T> : IColumnFilter
    {
        private readonly IColumnMetadata _column;
        private int _columnPartition = int.MinValue;
        private ITypedColumn<T> _columnReader;

        protected ColumnFilterBase(IColumnMetadata metadata)
        {
            _column = metadata;
        }

        public IColumnMetadata Column
        {
            get { return _column; }
        }

        protected abstract bool IsMatch(T value);
        protected abstract SingleMultipleValues<T> GetAllMatchingValues(IReadTransactionContext tx);
        public abstract long GetCardinality(IJournalCore journal, IReadTransactionContext tx);

        public bool IsMatch(IPartitionReader partition, IReadContext readCache, long localRowID)
        {
            if (_columnPartition != partition.PartitionID)
            {
                _columnPartition = partition.PartitionID;
                _columnReader = (ITypedColumn<T>)partition.ReadColumn(_column.ColumnID);
            }

            var value = _columnReader.Get(localRowID, readCache);
            return IsMatch(value);
        }


        public IEnumerable<long> Filter(IEnumerable<PartitionRowIDRange> partitions,
            IReadTransactionContext tx, ERowIDSortDirection sortDirection)
        {
            if (_column.Indexed)
            {
                var values = GetAllMatchingValues(tx);
                if (!values.IsNone)
                {
                    if (!values.IsSigle)
                    {
                        var valueSet = values.Values.Distinct().ToList();
                        var items = new IEnumerable<long>[valueSet.Count];
                        return partitions.SelectMany(part =>
                        {
                            var partition = tx.Read(part.PartitionID);
                            for (int v = 0; v < valueSet.Count; v++)
                            {
                                var symbolValue = valueSet[v];
                                var rowIDs = TakeFromTo(part, partition.GetSymbolRows(_column.ColumnID, symbolValue, tx));
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
                            var rowIDs = TakeFromTo(part, partition.GetSymbolRows(_column.ColumnID, values.Value, tx));
                            if (sortDirection == ERowIDSortDirection.Asc)
                            {
                                rowIDs = rowIDs.Reverse();
                            }
                            return rowIDs;
                        });
                    }
                }
            }

            return partitions.SelectMany(part =>
            {
                var partition = tx.Read(part.PartitionID);
                return IsPartitionMatch(tx, part, partition, sortDirection);
            });
        }

        private IEnumerable<long> IsPartitionMatch(IReadTransactionContext tx, PartitionRowIDRange part,
            IPartitionReader partition, ERowIDSortDirection sortDirection)
        {
            if (sortDirection == ERowIDSortDirection.Asc)
            {
                for (long rowId = part.Low; rowId <= part.High; rowId++)
                {
                    if (IsMatch(partition, tx.ReadCache, rowId))
                    {
                        yield return RowIDUtil.ToRowID(part.PartitionID, rowId);
                    }
                }
            }
            else
            {
                for (long rowId = part.High; rowId >= part.Low; rowId--)
                {
                    if (IsMatch(partition, tx.ReadCache, rowId))
                    {
                        yield return RowIDUtil.ToRowID(part.PartitionID, rowId);
                    }
                }
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

            public EnumerablePriorityQueue(int capcity, bool asc)
                : base(capcity)
            {
                _asc = asc;
            }

            protected override int Compare(IEnumerator<long> i1, IEnumerator<long> i2)
            {
                if (_asc && i1.Current > i2.Current) return 1;
                if (!_asc && i1.Current < i2.Current) return 1;
                return -1;
            }
        }

        public struct SingleMultipleValues<TT>
        {
            public readonly bool IsSigle;
            public readonly bool IsNone;
            public readonly TT Value;
            public readonly IList<TT> Values;
            public static readonly SingleMultipleValues<TT> NONE = new SingleMultipleValues<TT>(true);

            private SingleMultipleValues(bool none)
                : this()
            {
                IsNone = none;
            }

            private SingleMultipleValues(TT single) 
                : this()
            {
                IsSigle = true;
                Value = single;
            }

            private SingleMultipleValues(IList<TT> multiple)
                : this()
            {
                IsSigle = false;
                Values = multiple;
            }

            public static SingleMultipleValues<TT> Single(TT single)
            {
                return new SingleMultipleValues<TT>(single);
            }

            public static SingleMultipleValues<TT> Multiple(IList<TT> multiple)
            {
                return new SingleMultipleValues<TT>(multiple);
            }
        }
    }
}