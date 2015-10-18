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
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable.PlanItem
{
    public class LastestByPlanItem<T> : IPlanItem
    {
        private readonly ColumnMetadata _column;
        private long? _cardinality;
        private readonly T[] _keys;
        private IPlanItem _child;

        public LastestByPlanItem(ColumnMetadata column, IPlanItem child = null)
        {
            _column = column;
            _child = child;
            Timestamps = new DateRange();
        }

        public LastestByPlanItem(ColumnMetadata column, T[] literals, IPlanItem child = null)
            : this(column, child)
        {
            _keys = literals;
        }

        public IEnumerable<long> Execute(IJournalCore journal, IReadTransactionContext tx, ERowIDSortDirection sortDirection)
        {
            var intervalFilter = new PartitionIntervalIterator();
            var symbolFilter = new LatestByFilter<T>(journal, _column, _keys);
            
            return Timestamps.AllIntervals.Reverse().SelectMany(interval =>
                symbolFilter.Filter(intervalFilter.IteratePartitions(
                    tx.ReverseReadPartitions, interval, tx), tx, sortDirection)
                );
        }

        public long Cardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            if (!_cardinality.HasValue)
            {
                _cardinality = journal.QueryStatistics.GetColumnDistinctCardinality(tx, _column);
            }
            return _cardinality.Value;
        }

        public void Intersect(IPlanItem restriction)
        {
            Timestamps.Intersect(restriction.Timestamps);
        }

        public DateRange Timestamps { get; private set; }
    }
}