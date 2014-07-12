using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class PartitionIntervalIterator : IPartitionIntervalIterator
    {
        public IEnumerable<PartitionRowIDRange> IteratePartitions(IEnumerable<IPartitionCore> partitions,
            DateInterval interval, IReadTransactionContext tx)
        {
            foreach (var partt in partitions)
            {
                long low = int.MaxValue;
                long hi = int.MinValue;

                if (interval.Start < partt.StartDate)
                {
                    low = 0;
                }
                else if (partt.IsInsidePartition(interval.Start))
                {
                    low = partt.BinarySearchTimestamp(interval.Start, tx);
                    if (low < 0)
                    {
                        low = ~low;
                    }
                }

                // Interval and partition end days
                // are both exclusive
                if (interval.End >= partt.EndDate)
                {
                    hi = tx.GetRowCount(partt.PartitionID) - 1;
                }
                else if (partt.IsInsidePartition(interval.End))
                {
                    hi = partt.BinarySearchTimestamp(interval.End, tx);
                    if (hi < 0)
                    {
                        hi = ~hi - 1;
                    }
                    else
                    {
                        hi = hi - 1;
                    }
                }

                if (low <= hi)
                {
                    yield return new PartitionRowIDRange(partt, low, hi);
                }
            }
        }
    }
}