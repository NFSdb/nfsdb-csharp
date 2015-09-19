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
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable.PlanItem
{
    public class RowScanPlanItem : IColumnScanPlanItemCore, IPlanItem
    {
        private readonly IJournalCore _journal;
        private readonly IReadTransactionContext _tx;
        private List<IColumnFilter> _filters;
        private IPartitionFilter _partitionFilter;
        private long? _cardin;

        public RowScanPlanItem(IJournalCore journal, IReadTransactionContext tx)
        {
            _journal = journal;
            _tx = tx;
            Timestamps = new DateRange();
        }

        public void AddContainsScan<T>(ColumnMetadata column, T[] literals)
        {
            var newFilter = new SymbolFilter<T>(column, literals);
            AddFilter(column, newFilter);
        }

        public void AddContainsScan<T>(ColumnMetadata column, T literal)
        {
            var newFilter = new SymbolFilter<T>(column, literal);
            AddFilter(column, newFilter);
        }

        private void AddFilter<T>(ColumnMetadata column, SymbolFilter<T> newFilter)
        {
            if (column.Indexed)
            {
                var partitionAsColumn = _partitionFilter as IColumnFilter;
                if (_partitionFilter == null ||
                    (partitionAsColumn != null
                     && partitionAsColumn.GetCardinality(_journal, _tx) > newFilter.GetCardinality(_journal, _tx)))
                {
                    if (partitionAsColumn != null)
                    {
                        _filters.Add(partitionAsColumn);
                    }
                    _partitionFilter = newFilter;
                }
            }
            else
            {
                if (_filters == null) _filters = new List<IColumnFilter>();
                _filters.Add(newFilter);
            }
        }

        public void AddFilter(IColumnFilter filter)
        {
            if (_filters == null) _filters = new List<IColumnFilter>();
            _filters.Add(filter);
        }

        public string SymbolName
        {
            get
            {
                if (_filters != null && _filters.Count == 1)
                {
                    return _filters[0].Column;
                }
                return null;
            }
        }

        public IEnumerable<long> Execute(IJournalCore journal, IReadTransactionContext tx, 
            ERowIDSortDirection sort)
        {
            var intervalFilter = new PartitionIntervalIterator();
            var intervals = Timestamps.AllIntervals.SelectMany(
                i => intervalFilter.IteratePartitions(tx.ReadPartitions, i, tx)).ToList();

            if (sort == ERowIDSortDirection.Desc)
            {
                intervals.Reverse();
            }

            if (_partitionFilter != null)
            {
                if (_filters == null)
                {
                    return _partitionFilter.Filter(intervals, tx, sort);
                }
                OptimizeFilters(journal, tx);
                return ApplyFilters(_partitionFilter.Filter(intervals, tx, sort), tx);
            }
            OptimizeFilters(journal, tx);
            return FilterRowsRange(tx, intervals, sort);
        }

        private void OptimizeFilters(IJournalCore journal, IReadTransactionContext tx)
        {
            if (_filters != null)
            {
                _filters.Sort(new DescendingCardinalityComparer(journal, tx));
            }
        }

        private IEnumerable<long> FilterRowsRange(IReadTransactionContext tx, List<PartitionRowIDRange> intervals, ERowIDSortDirection sort)
        {
            for (int i = 0; i < intervals.Count; i++)
            {
                var partitionRowIDRange = intervals[i];
                var paritition = tx.Read(partitionRowIDRange.PartitionID);
                long from = sort == ERowIDSortDirection.Asc ? partitionRowIDRange.Low : partitionRowIDRange.High;
                long to = sort == ERowIDSortDirection.Asc ? partitionRowIDRange.High: partitionRowIDRange.Low;
                long increment = sort == ERowIDSortDirection.Asc ? 1 : -1;

                for (long rowID = from; rowID < to; rowID += increment)
                {
                    if (MatchFilters(paritition, tx.ReadCache, rowID))
                    {
                        yield return RowIDUtil.ToRowID(partitionRowIDRange.PartitionID, rowID);
                    }
                }
            }
        }

        private IEnumerable<long> ApplyFilters(IEnumerable<long> rowIDs, IReadTransactionContext tx)
        {
            int partitionIndex = -1;
            IPartitionReader partition = null;
            foreach (var globalRowID in rowIDs)
            {
                var rowPartitionIndex = RowIDUtil.ToPartitionIndex(globalRowID);
                if (rowPartitionIndex != partitionIndex)
                {
                    partition = tx.Read(partitionIndex);
                    partitionIndex = rowPartitionIndex;
                }

                if (MatchFilters(partition, tx.ReadCache, RowIDUtil.ToLocalRowID(globalRowID)))
                {
                    yield return globalRowID;
                }
            }
        }

        private bool MatchFilters(IPartitionReader partition, IReadContext readContext, long localRowID)
        {
            if (_filters == null) return true;
            for (int i = 0; i < _filters.Count; i++)
            {
                if (!_filters[i].IsMatch(partition, readContext,localRowID))
                {
                    return false;
                }
            }
            return true;
        }

        public long Cardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            if (!_cardin.HasValue)
            {
                if (_filters != null)
                {
                    _cardin = _filters.Min(f => f.GetCardinality(journal, tx));
                }
                else
                {
                    _cardin = long.MaxValue;
                }
            }
            return _cardin.Value;
        }

        public void Intersect(IPlanItem restriction)
        {
            Timestamps.Intersect(restriction.Timestamps);
        }

        public DateRange Timestamps { get; private set; }

        public bool CanTranformLastestByIdPlanItem(ColumnMetadata column)
        {
            var partitionAsColumn = _partitionFilter as IColumnFilter;
            if (_partitionFilter != null && partitionAsColumn != null)
            {
                return false;
            }

            return true;
        }

        public void TranformLastestByIdPlanItem(ColumnMetadata column)
        {
            if (!CanTranformLastestByIdPlanItem(column))
            {
                throw new InvalidOperationException("Please check CanTranformLastestByIdPlanItem first.");
            }

            var partitionAsColumn = _partitionFilter as IColumnFilter;
            if (partitionAsColumn != null)
            {
                _filters.Add(partitionAsColumn);
            }

            switch (column.FieldType)
            {
                case EFieldType.Byte:
                    _partitionFilter = new LatestBySymbolFilter<byte>(_journal, column, ExtractColumnContains<byte>(column));
                    break;
                case EFieldType.Bool:
                    _partitionFilter = new LatestBySymbolFilter<bool>(_journal, column, ExtractColumnContains<bool>(column));
                    break;
                case EFieldType.Int16:
                    _partitionFilter = new LatestBySymbolFilter<short>(_journal, column, ExtractColumnContains<short>(column));
                    break;
                case EFieldType.Int32:
                    _partitionFilter = new LatestBySymbolFilter<int>(_journal, column, ExtractColumnContains<int>(column));
                    break;
                case EFieldType.Int64:
                    _partitionFilter = new LatestBySymbolFilter<long>(_journal, column, ExtractColumnContains<long>(column));
                    break;
                case EFieldType.Double:
                    _partitionFilter = new LatestBySymbolFilter<double>(_journal, column, ExtractColumnContains<double>(column));
                    break;
                case EFieldType.Symbol:
                case EFieldType.String:
                    _partitionFilter = new LatestBySymbolFilter<string>(_journal, column, ExtractColumnContains<string>(column));
                    break;
                case EFieldType.DateTime:
                case EFieldType.DateTimeEpochMilliseconds:
                    _partitionFilter = new LatestBySymbolFilter<DateTime>(_journal, column, ExtractColumnContains<DateTime>(column));
                    break;
                default:
                    throw new NFSdbQuaryableNotSupportedException("Latest by bit set column is not supported.");
            }
        }

        private T[] ExtractColumnContains<T>(ColumnMetadata column)
        {
            SymbolFilter<T> found = _filters
                .OfType<SymbolFilter<T>>()
                .FirstOrDefault(c => c.Column == column.PropertyName);

            if (found != null)
            {
                _filters.Remove(found);
                return found.FilterValues;
            }
            return null;
        }

        public bool TryIntersect(RowScanPlanItem rowScan2)
        {
            if (_partitionFilter == null || rowScan2._partitionFilter == null)
            {
                _partitionFilter = _partitionFilter ?? rowScan2._partitionFilter;
                _filters.AddRange(rowScan2._filters);
                return true;
            }
            return false;
        }
    }

    public class DescendingCardinalityComparer : IComparer<IColumnFilter>
    {
        private readonly IJournalCore _journal;
        private readonly IReadTransactionContext _rtx;

        public DescendingCardinalityComparer(IJournalCore journal, IReadTransactionContext rtx)
        {
            _journal = journal;
            _rtx = rtx;
        }

        public int Compare(IColumnFilter x, IColumnFilter y)
        {
            return y.GetCardinality(_journal, _rtx).CompareTo(x.GetCardinality(_journal, _rtx));
        }
    }
}