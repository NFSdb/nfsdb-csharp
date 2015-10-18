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
        private List<IColumnFilter> _andFilters;
        private IPartitionFilter _partitionFilter;
        private long? _cardin;

        public RowScanPlanItem(IJournalCore journal, IReadTransactionContext tx)
        {
            _journal = journal;
            _tx = tx;
            Timestamps = new DateRange();
        }

        public void AddContainsScan<T>(ColumnMetadata column, IList<T> literals)
        {
            var newFilter = new SymbolFilter<T>(column, literals);
            AddFilter(newFilter);
        }

        public void AddContainsScan<T>(ColumnMetadata column, T literal)
        {
            var newFilter = new SymbolFilter<T>(column, literal);
            AddFilter(newFilter);
        }

        private void AddFilter<T>(SymbolFilter<T> newFilter)
        {
            var partitionAsColumn = _partitionFilter as IColumnFilter;
            if (_partitionFilter == null ||
                (partitionAsColumn != null
                 && partitionAsColumn.GetCardinality(_journal, _tx) > newFilter.GetCardinality(_journal, _tx)))
            {
                if (partitionAsColumn != null)
                {
                    _andFilters.Add(partitionAsColumn);
                }
                _partitionFilter = newFilter;
            }
        }

        public void AddFilter(IColumnFilter filter)
        {
            if (_andFilters == null) _andFilters = new List<IColumnFilter>();
            _andFilters.Add(filter);
        }

        public string LatestSymbolName
        {
            get
            {
                var filter = _partitionFilter as ILatestBySymbolFilter;
                if (filter != null)
                {
                    return filter.Column.PropertyName;
                }
                return null;
            }
        }

        internal IEnumerable<IColumnFilter> ColumnFilters
        {
            get { return _andFilters; }
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
                if (_andFilters == null)
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
            if (_andFilters != null)
            {
                _andFilters.Sort(new DescendingCardinalityComparer(journal, tx));
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
                    partition = tx.Read(rowPartitionIndex);
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
            if (_andFilters == null) return true;
            for (int i = 0; i < _andFilters.Count; i++)
            {
                if (!_andFilters[i].IsMatch(partition, readContext,localRowID))
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
                if (_andFilters != null)
                {
                    _cardin = _andFilters.Min(f => f.GetCardinality(journal, tx));
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
            if (_partitionFilter != null && partitionAsColumn == null)
            {
                var existingLatestFileter = _partitionFilter as ILatestBySymbolFilter;
                return existingLatestFileter != null && existingLatestFileter.Column.FieldID == column.FieldID;
            }

            return true;
        }

        public void ApplyLastestByIdPlanItem(ColumnMetadata column)
        {
            if (!CanTranformLastestByIdPlanItem(column))
            {
                throw new InvalidOperationException("Please check CanTranformLastestByIdPlanItem first.");
            }

            var existingLatestFileter = _partitionFilter as ILatestBySymbolFilter;
            if (existingLatestFileter != null && existingLatestFileter.Column.FieldID == column.FieldID)
            {
                return;
            }

            var partitionAsColumn = _partitionFilter as IColumnFilter;
            if (partitionAsColumn != null)
            {
                if (_andFilters == null) _andFilters = new List<IColumnFilter>();
                _andFilters.Add(partitionAsColumn);
                _partitionFilter = null;
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
                    throw new NFSdbQueryableNotSupportedException("Latest by {0} column is not supported.", column.FieldType);
            }
        }

        private IList<T> ExtractColumnContains<T>(ColumnMetadata column)
        {
            var mainF = _partitionFilter as SymbolFilter<T>;
            if (mainF != null && mainF.Column.FieldID == column.FieldID)
            {
                return mainF.FilterValues;
            }

            if (_andFilters != null)
            {
                SymbolFilter<T> found = _andFilters
                    .OfType<SymbolFilter<T>>()
                    .FirstOrDefault(c => c.Column.FieldID == column.FieldID);

                if (found != null)
                {
                    _andFilters.Remove(found);
                    return found.FilterValues;
                }
            }
            return null;
        }

        public bool TryIntersect(RowScanPlanItem rowScan2)
        {
            if (_partitionFilter is IColumnFilter && rowScan2._partitionFilter is IColumnFilter)
            {
                if (_andFilters == null) _andFilters = new List<IColumnFilter>();
                var thisMain = (IColumnFilter)_partitionFilter;
                var thatMain = (IColumnFilter)rowScan2._partitionFilter;

                if (thisMain.GetCardinality(_journal, _tx) > thatMain.GetCardinality(_journal, _tx))
                {
                    _andFilters.Add(thisMain);
                    _partitionFilter = rowScan2._partitionFilter;
                    if (rowScan2._andFilters != null)
                    {
                        _andFilters.AddRange(rowScan2._andFilters);
                    }
                }
                else
                {
                    _andFilters.Add(thatMain);
                    if (rowScan2._andFilters != null)
                    {
                        _andFilters.AddRange(rowScan2._andFilters);
                    }
                }
                return true;
            }
            else if (_partitionFilter is IColumnFilter)
            {
                if (_andFilters == null) _andFilters = new List<IColumnFilter>();
                var thisMain = (IColumnFilter)_partitionFilter;
                _andFilters.Add(thisMain);
                _partitionFilter = rowScan2._partitionFilter;
                if (rowScan2._andFilters != null)
                {
                    _andFilters.AddRange(rowScan2._andFilters);
                }
                return true;
            }
            else if (rowScan2._partitionFilter is IColumnFilter)
            {
                if (_andFilters == null) _andFilters = new List<IColumnFilter>();
                var thatMain = (IColumnFilter)rowScan2._partitionFilter;
                _andFilters.Add(thatMain);
                if (rowScan2._andFilters != null)
                {
                    _andFilters.AddRange(rowScan2._andFilters);
                }
                return true;
            }
            return false;
        }

        public override string ToString()
        {
            string andFiltersString = null;
            if (_andFilters != null)
            {
                andFiltersString = string.Join(" and ", _andFilters.Select(f => f.ToString()));
            }

            if (_partitionFilter != null)
            {
                if (_andFilters != null && _andFilters.Count > 0)
                {
                    return string.Join(" and ", _partitionFilter, andFiltersString);
                }
                return _partitionFilter.ToString();
            }
            return andFiltersString ?? string.Empty;
        }

        public bool TryUnion(RowScanPlanItem rowScan2)
        {
            var c1 = GetOnlySymbolScan();
            var c2 = rowScan2.GetOnlySymbolScan();
            if (c1 != null && c2 != null && c1.FieldID == c2.FieldID)
            {
                switch (c1.FieldType)
                {
                    case EFieldType.Byte:
                        _partitionFilter = new SymbolFilter<byte>(c1,
                            ExtractColumnContains<byte>(c1).Concat(rowScan2.ExtractColumnContains<byte>(c2))
                            .Distinct().ToArray());
                        break;
                    case EFieldType.Bool:
                        _partitionFilter = new SymbolFilter<bool>(c1,
                            ExtractColumnContains<bool>(c1).Concat(rowScan2.ExtractColumnContains<bool>(c2))
                            .Distinct().ToArray());
                        break;
                    case EFieldType.Int16:
                        _partitionFilter = new SymbolFilter<Int16>(c1,
                            ExtractColumnContains<Int16>(c1).Concat(rowScan2.ExtractColumnContains<Int16>(c2))
                            .Distinct().ToArray());
                        break;
                    case EFieldType.Int32:
                        _partitionFilter = new SymbolFilter<int>(c1,
                            ExtractColumnContains<int>(c1).Concat(rowScan2.ExtractColumnContains<int>(c2))
                            .Distinct().ToArray());
                        break;
                    case EFieldType.Int64:
                        _partitionFilter = new SymbolFilter<long>(c1,
                            ExtractColumnContains<long>(c1).Concat(rowScan2.ExtractColumnContains<long>(c2))
                            .Distinct().ToArray());
                        break;
                    case EFieldType.Double:
                        _partitionFilter = new SymbolFilter<double>(c1,
                            ExtractColumnContains<double>(c1)
                                .Concat(rowScan2.ExtractColumnContains<double>(c2))
                                .Distinct().ToArray());
                        break;
                    case EFieldType.Symbol:
                    case EFieldType.String:
                        _partitionFilter = new SymbolFilter<string>(c1,
                            ExtractColumnContains<string>(c1)
                                .Concat(rowScan2.ExtractColumnContains<string>(c2))
                                .Distinct().ToArray());
                        break;
                    case EFieldType.DateTime:
                    case EFieldType.DateTimeEpochMilliseconds:
                        _partitionFilter = new SymbolFilter<DateTime>(c1,
                            ExtractColumnContains<DateTime>(c1)
                                .Concat(rowScan2.ExtractColumnContains<DateTime>(c2))
                                .Distinct().ToArray());
                        break;
                    default:
                        return false;
                }
                return true;
            }

            return false;
        }

        private ColumnMetadata GetOnlySymbolScan()
        {
            if (_partitionFilter is IColumnFilter && (_andFilters == null || _andFilters.Count == 0))
            {
                return ((IColumnFilter) _partitionFilter).Column;
            }
            return null;
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