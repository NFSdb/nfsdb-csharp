﻿#region copyright
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
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable.PlanItem
{
    public class ColumnScanPlanItem<T> : IColumnScanPlanItemCore, IPlanItem
    {
        private readonly ColumnMetadata _column;
        private readonly T[] _literals;
        private long? _cardin;

        public ColumnScanPlanItem(ColumnMetadata column, T[] literals)
        {
            _column = column;
            _literals = literals;
            Timestamps = new DateRange();
        }

        public string SymbolName { get { return _column.PropertyName; } }

        public IEnumerable<long> Execute(IJournalCore journal, IReadTransactionContext tx, 
            ERowIDSortDirection sort)
        {
            var intervalFilter = new PartitionIntervalIterator();
            var symbolFilter = new SymbolFilter<T>(_column, _literals);
            var intervals = Timestamps.AllIntervals.SelectMany(
                i => intervalFilter.IteratePartitions(tx.ReadPartitions, i, tx)).ToList();

            if (sort == ERowIDSortDirection.Desc)
                intervals.Reverse();

            return symbolFilter.Filter(intervals, tx, sort);
        }

        public T[] Literals {get { return _literals; }}

        public long Cardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            if (!_cardin.HasValue)
            {
                _cardin = journal.QueryStatistics.RowsBySymbolValue(tx, _column, _literals);
            }
            return _cardin.Value;
        }

        public void Intersect(IPlanItem restriction)
        {
            Timestamps.Intersect(restriction.Timestamps);
        }

        public DateRange Timestamps { get; private set; }

        public IPlanItem ToLastestByIdPlanItem()
        {
            return new LastestByIdPlanItem<T>(_column, _literals);
        }
    }
}