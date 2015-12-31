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
using System.Globalization;
using System.Linq;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Core.Writes;
using Apaf.NFSdb.Tests.Tx;
using Moq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Query
{
    [TestFixture]
    public class PartitionIntervalIteratorTests
    {
        private TransactionContext _tx;

        [TestCase(null, null, ExpectedResult = "1,2,3,4,5")]
        [TestCase(null, "2014-05-01", ExpectedResult = "1,2,3,4,5")]
        [TestCase(null, "2014-04-03", ExpectedResult = "1,2")]
        [TestCase("2014-04-03", "2014-04-03", ExpectedResult = "")]
        [TestCase("2014-04-03", null, ExpectedResult = "3,4,5")]
        [TestCase("2014-04-01", "2014-04-06", ExpectedResult = "1,2,3,4,5")]
        public string Should_find_paritions_by_interval(string startStr, string endStr)
        {
            var iter = CreateIterator();
            var partitions = CreatePartitions(new DateTime(2014, 04, 01), 
                new DateTime(2014, 04, 06), 5, TimeSpan.FromHours(1));

            var start = ToDateTime(startStr);
            var end = ToDateTime(endStr);
            DateInterval di = DateInterval.Any;
            if (start != null && end != null)
            {
                di = new DateInterval(start.Value, end.Value);
            }
            else if (start != null)
            {
                di = DateInterval.From(start.Value);
            }
            else if (end != null)
            {
                di = DateInterval.To(end.Value);
            }

            var resultIter = 
                iter.IteratePartitions(partitions, di, _tx);
            
            return string.Join(",",
                resultIter.Select(p => string.Format("{0}", p.PartitionID)));
        }


        [TestCase(null, null, ExpectedResult = "0-23")]
        [TestCase("2014-04-01 00:00:01", null, ExpectedResult = "1-23")]
        [TestCase("2014-04-01 05:00:01", null, ExpectedResult = "6-23")]
        [TestCase("2014-04-01 05:00:00", null, ExpectedResult = "5-23")]
        [TestCase("2014-04-01 05:00:00", "2014-04-01 06:00:00", ExpectedResult = "5-5")]
        public string Should_find_parition_hi_lo(string startStr, string endStr)
        {
            var iter = CreateIterator();
            var partitions = CreatePartitions(new DateTime(2014, 04, 01),
                new DateTime(2014, 04, 02), 1, TimeSpan.FromHours(1));

            var start = ToDateTime(startStr);
            var end = ToDateTime(endStr);
            DateInterval di = DateInterval.Any;
            if (start != null && end != null)
            {
                di = new DateInterval(start.Value, end.Value);
            }
            else if (start != null)
            {
                di = DateInterval.From(start.Value);
            }
            else if (end != null)
            {
                di = DateInterval.To(end.Value);
            }

            var resultIter =
                iter.IteratePartitions(partitions, di, _tx);

            return string.Join(",",
                resultIter.Select(p => string.Format("{0}-{1}", p.Low, p.High)));
        }


        private DateTime? ToDateTime(string startStr)
        {
            if (startStr == null) return null;
            return DateTime.Parse(startStr, CultureInfo.InvariantCulture);
        }

        private PartitionIntervalIterator CreateIterator()
        {
            return new PartitionIntervalIterator();
        }

        private IPartition[] CreatePartitions(DateTime from, DateTime to, int count, TimeSpan recordsDelay)
        {
            var res = new List<IPartition>();
            var tsmp = DateUtils.DateTimeToUnixTimeStamp(from);
            var end = DateUtils.DateTimeToUnixTimeStamp(to);
            var partStart = tsmp;
            var partIncr = (long)(to - from).TotalMilliseconds/count;
            var recIncr = (long)recordsDelay.TotalMilliseconds;
            var partId = 1;
            var partTxs = new List<PartitionTxData>();
            partTxs.Add(new PartitionTxData(100, 0, DateTime.MinValue, DateTime.MaxValue));

            while (tsmp < end)
            {
                var partTsmps = new List<long>();
                while (tsmp < partStart + partIncr)
                {
                    partTsmps.Add(tsmp); 
                    tsmp += recIncr;
                }
                partTxs.Add(new PartitionTxData(100, partId,
                    DateUtils.UnixTimestampToDateTime(partStart), 
                    DateUtils.UnixTimestampToDateTime(partStart + partIncr)));

                partTxs[partId].NextRowID = partTsmps.Count;
                res.Add(MockPartition(partTsmps, partStart, partStart + partIncr, partId++));
                partStart += partIncr;
            }
            _tx = new TransactionContext(100, partTxs.ToArray(), res.ToArray(), new Mock<ITxPartitionLock>().Object);

            return res.ToArray();
        }

        private IPartition MockPartition(List<long> partTsmps, long start, long end, int id)
        {
            var part = new Mock<IPartition>();
            part.Setup(p => p.StartDate).Returns(DateUtils.UnixTimestampToDateTime(start));
            part.Setup(p => p.EndDate).Returns(DateUtils.UnixTimestampToDateTime(end));
            part.Setup(p => p.PartitionID).Returns(id);
            part.Setup(p => p.BinarySearchTimestamp(It.IsAny<DateTime>(), It.IsAny<IReadTransactionContext>()))
                .Returns((DateTime dt, IReadTransactionContext tx) => 
                    partTsmps.BinarySearch(DateUtils.DateTimeToUnixTimeStamp(dt)));
            return part.Object;
        }
    }
}