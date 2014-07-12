#region copyright
/*
 * Copyright (c) 2014. APAF (Alex Pelagenko).
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
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable.PlanItem
{
    public class LastestByIdPlanItem : IPlanItem
    {
        private readonly string _latestBySymbol;
        private long? _cardinality;
        private string[] _keys;

        public LastestByIdPlanItem(string latestBySymbol)
        {
            _latestBySymbol = latestBySymbol;
            Timestamps = new DateRange();
        }

        public LastestByIdPlanItem(string latestBySymbol, string[] literals)
            : this(latestBySymbol)
        {
            _keys = literals;
        }

        public IEnumerable<long> Execute(IJournalCore journal, IReadTransactionContext tx)
        {
            var intervalFilter = new PartitionIntervalIterator();
            var symbolFilter = new LatestBySymbolFilter(journal, _latestBySymbol, _keys);

            return Timestamps.AllIntervals.Reverse().SelectMany(interval =>
                symbolFilter.Filter(intervalFilter.IteratePartitions(
                    journal.PartitionsCore.Reverse(), interval, tx), tx)
                );
        }

        public long Cardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            if (!_cardinality.HasValue)
            {
                _cardinality = journal.QueryStatistics.GetSymbolCount(tx, _latestBySymbol);
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