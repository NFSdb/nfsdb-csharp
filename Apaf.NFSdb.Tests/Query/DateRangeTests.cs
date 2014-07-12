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
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Queries.Queryable;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Query
{
    [TestFixture]
    public class DateRangeTests
    {
        [TestCase("null,10", "5,10", ExpectedResult = "5,10")]
        [TestCase("0,10", "5,10", ExpectedResult = "5,10")]
        [TestCase("0,10", "5,10", ExpectedResult = "5,10")]
        [TestCase("0,10", "5,20", ExpectedResult = "5,10")]
        [TestCase("0,10", "10,20", ExpectedResult = "")]
        [TestCase("0,10#20,30", "5,20#21,101", ExpectedResult = "5,10#21,30")]
        [TestCase("0,10#20,30#50,60", "5,20#21,101", ExpectedResult = "5,10#21,30#50,60")]
        [TestCase("5,10", "0,10", ExpectedResult = "5,10")]
        [TestCase("5,10", "0,10", ExpectedResult = "5,10")]
        [TestCase("5,20", "0,10", ExpectedResult = "5,10")]
        [TestCase("10,20", "0,10", ExpectedResult = "")]
        [TestCase("5,20#21,101", "0,10#20,30", ExpectedResult = "5,10#21,30")]
        [TestCase("5,20#21,101", "0,10#20,30#50,60", ExpectedResult = "5,10#21,30#50,60")]
        [TestCase("5,20#21,null", "0,10#20,30#50,60", ExpectedResult = "5,10#21,30#50,60")]
        [TestCase("0,10#20,30#50,60", "null,null", ExpectedResult = "0,10#20,30#50,60")]
        public string Intersect(string first, string second)
        {
            var range1 = CreateDateRange(first);
            var range2 = CreateDateRange(second);

            range1.Intersect(range2);

            return string.Join("#", range1.AllIntervals.Select(
                intv => intv.Start.Ticks + "," + intv.End.Ticks));
        }

        [TestCase("1,10", "5,10", ExpectedResult = "1,10")]
        [TestCase("1,10", "null,null", ExpectedResult = "null,null")]
        [TestCase("2,10", "5,20", ExpectedResult = "2,20")]
        [TestCase("null,10", "5,20#30,null", ExpectedResult = "null,20#30,null")]
        [TestCase("null,10#15,30#35,100", "5,20#25,40", ExpectedResult = "null,100")]
        [TestCase("10,20#40,50", "10,20", ExpectedResult = "10,20#40,50")]
        public string Union(string first, string second)
        {
            var range1 = CreateDateRange(first);
            var range2 = CreateDateRange(second);

            range1.Union(range2);

            return string.Join("#", range1.AllIntervals.Select(
                intv => ToTicksString(intv.Start.Ticks) + "," + ToTicksString(intv.End.Ticks)
                ));
        }

        private static string ToTicksString(long intv)
        {
            return intv == DateTime.MinValue.Ticks || intv == DateTime.MaxValue.Ticks
                ? "null"
                : intv.ToString(CultureInfo.InvariantCulture);
        }

        private DateRange CreateDateRange(string first)
        {
            var intList = new List<DateInterval>();
            var ints = first.Split('#');
            foreach (var strInt in ints)
            {
                var numbers = strInt.Split(',');
                if (numbers.Length != 2)
                {
                    throw new ArgumentException(numbers.Length.ToString(CultureInfo.InvariantCulture));
                }

                intList.Add(new DateInterval(ToStartDateTime(numbers[0]), ToEndDateTime(numbers[1])));
            }
            return new DateRange(intList);
        }

        private DateTime ToStartDateTime(string strDate)
        {
            if (strDate == "null")
            {
                return DateTime.MinValue;
            }
            return new DateTime(long.Parse(strDate));
        }

        private DateTime ToEndDateTime(string strDate)
        {
            if (strDate == "null")
            {
                return DateTime.MaxValue;
            }
            return new DateTime(long.Parse(strDate));
        }
    }
}