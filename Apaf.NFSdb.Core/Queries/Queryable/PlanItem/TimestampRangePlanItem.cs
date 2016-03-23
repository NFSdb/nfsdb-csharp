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

        public IEnumerable<long> Execute(IJournalCore journal, IReadTransactionContext tx, ERowIDSortDirection sort)
        {
            var intervalFilter = new PartitionIntervalIterator();
            if (sort == ERowIDSortDirection.Desc)
            {
                return Timestamps.AllIntervals.Reverse().SelectMany(
                    interval => GetIdsDesc(
                        intervalFilter.IteratePartitions(
                            tx.ReadPartitions.Reverse(), interval, tx))
                    );
            }
            return Timestamps.AllIntervals.SelectMany(
                interval => GetIdsAsc(
                    intervalFilter.IteratePartitions(tx.ReadPartitions, interval, tx))
                );
        }

        private static IEnumerable<long> GetIdsDesc(IEnumerable<PartitionRowIDRange> parititionsFiltered)
        {
            foreach (var idRange in parititionsFiltered)
            {
                for (long l = idRange.High; l >= idRange.Low; l--)
                {
                    yield return RowIDUtil.ToRowID(idRange.PartitionID, l);
                }
            }
        }

        private static IEnumerable<long> GetIdsAsc(IEnumerable<PartitionRowIDRange> parititionsFiltered)
        {
            foreach (var idRange in parititionsFiltered)
            {
                for (long l = idRange.Low; l <= idRange.High; l++)
                {
                    yield return RowIDUtil.ToRowID(idRange.PartitionID, l);
                }
            }
        }

        public long Cardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            if (!_caridnality.HasValue)
            {
                var intervalFilter = new PartitionIntervalIterator();
                _caridnality =
                    Timestamps
                    .AllIntervals
                    .Sum(p => intervalFilter.IteratePartitions(tx.ReadPartitions, p, tx).Sum(p2 => p2.High - p2.Low));
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