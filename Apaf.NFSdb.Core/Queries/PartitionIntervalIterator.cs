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
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class PartitionIntervalIterator : IPartitionIntervalIterator
    {
        public IList<PartitionRowIDRange> IteratePartitions(IEnumerable<IPartitionReader> partitions,
            DateInterval interval, IReadTransactionContext tx)
        {
            if (DateInterval.Any.Equals(interval))
            {
                return
                    partitions.Select(p => new PartitionRowIDRange(p.PartitionID, 0, tx.GetRowCount(p.PartitionID) - 1))
                        .ToList();
            }

            var result = new List<PartitionRowIDRange>();
            foreach (var reader in partitions)
            {
                if (reader == null) continue;

                long low = long.MaxValue;
                long hi = long.MinValue;
                int partitionID = reader.PartitionID;
                var partt = tx.GetPartitionTx(partitionID);

                if (interval.Start < partt.StartDate)
                {
                    low = 0;
                }
                else if (partt.StartDate <= interval.Start && interval.Start < partt.EndDate)
                {
                    // ReSharper disable once PossibleNullReferenceException
                    low = reader.BinarySearchTimestamp(interval.Start, tx);
                    if (low < 0)
                    {
                        low = ~low;
                    }
                }

                // Interval and partition end days
                // are both exclusive
                if (interval.End >= partt.EndDate)
                {
                    hi = tx.GetRowCount(partitionID) - 1;
                }
                else if (partt.StartDate <= interval.End && interval.End < partt.EndDate)
                {
                    // ReSharper disable once PossibleNullReferenceException
                    hi = reader.BinarySearchTimestamp(interval.End, tx);
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
                    result.Add(new PartitionRowIDRange(partitionID, low, hi));
                }
            }
            return result;
        }
    }
}