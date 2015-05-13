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
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class PartitionIntervalIterator : IPartitionIntervalIterator
    {
        public IEnumerable<PartitionRowIDRange> IteratePartitions(IEnumerable<int> partitionsIDs,
            DateInterval interval, IReadTransactionContext tx)
        {
            foreach (var partitionID in partitionsIDs)
            {
                long low = long.MaxValue;
                long hi = long.MinValue;
                var partt = tx.GetPartitionTx(partitionID);

                if (interval.Start < partt.StartDate)
                {
                    low = 0;
                }
                else if (partt.StartDate <= interval.Start && interval.Start < partt.EndDate)
                {
                    using (var reader = tx.Read(partitionID))
                    {
                        low = reader.BinarySearchTimestamp(interval.Start, tx);
                        if (low < 0)
                        {
                            low = ~low;
                        }
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
                    using (var reader = tx.Read(partitionID))
                    {
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
                }

                if (low <= hi)
                {
                    yield return new PartitionRowIDRange(partitionID, low, hi);
                }
            }
        }
    }
}