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