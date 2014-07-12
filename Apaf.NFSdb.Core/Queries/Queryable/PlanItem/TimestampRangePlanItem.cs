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
    public class TimestampRangePlanItem : IPlanItem
    {
        private long? _caridnality;

        public TimestampRangePlanItem(DateInterval filterInterval)
        {
            Timestamps = DateRange.FromInterval(filterInterval);
        }

        public IEnumerable<long> Execute(IJournalCore journal, IReadTransactionContext tx)
        {
            var intervalFilter = new PartitionIntervalIterator();
            return Timestamps.AllIntervals.Reverse().SelectMany(
                interval => GetIds(
                    intervalFilter.IteratePartitions(
                        journal.PartitionsCore.Reverse(), interval, tx))
            );
        }

        private IEnumerable<long> GetIds(IEnumerable<PartitionRowIDRange> parititionsFiltered)
        {
            foreach (var idRange in parititionsFiltered)
            {
                for (long l = idRange.High; l >= idRange.Low; l--)
                {
                    yield return RowIDUtil.ToRowID(idRange.Partition.PartitionID, l);
                }
            }
        }

        public long Cardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            if (!_caridnality.HasValue)
            {
                _caridnality = Execute(journal, tx).Count();
            }
            return _caridnality.Value;
        }

        public void Intersect(IPlanItem restriction)
        {
            Timestamps.Intersect(restriction.Timestamps);
        }

        public DateRange Timestamps { get; private set; }
    }
}