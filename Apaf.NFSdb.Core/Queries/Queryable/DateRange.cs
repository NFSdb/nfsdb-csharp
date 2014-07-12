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
using System.Linq;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class DateRange
    {
        private List<DateInterval> _intervals;

        public DateRange()
        {
            _intervals = new List<DateInterval>
            {
                DateInterval.Any
            };
        }

        public DateRange(IEnumerable<DateInterval> interval)
        {
            _intervals = interval.OrderBy(i => i.Start).ToList();
        }

        public DateRange(DateRange timestamps)
        {
            _intervals = new List<DateInterval>(timestamps._intervals);
        }

        public static DateRange FromInterval(DateInterval intv)
        {
            var r = new DateRange();
            r._intervals[0] = intv;
            return r;
        }

        public IEnumerable<DateInterval> AllIntervals
        {
            get { return _intervals; }
        }

        public void Intersect(DateRange intv)
        {
            int i = 0;
            int j = 0;
            var intersection = new List<DateInterval>();
            while (i < _intervals.Count && j < intv._intervals.Count)
            {
                var iR = _intervals[i];
                var jR = intv._intervals[j];
                var start = Max(iR.Start, jR.Start);
                var end = Min(iR.End, jR.End);
                if (start < end)
                {
                    intersection.Add(new DateInterval(start, end));
                }

                if (iR.End < jR.End)
                {
                    i++;
                }
                else
                {
                    j++;
                }
            }
            _intervals = intersection;
        }

        public void Union(DateRange intv)
        {
            int i = 0;
            int j = 0;
            var iCount = _intervals.Count;
            var jCount = intv._intervals.Count;
            var union = new List<DateInterval>();

            DateTime intStart = DateTime.MaxValue;
            DateTime intEnd = DateTime.MaxValue;
            while (i < _intervals.Count || j < intv._intervals.Count)
            {
                DateInterval next;
                if (i < iCount && j < jCount)
                {
                    var iR = _intervals[i];
                    var jR = intv._intervals[j];
                    if (iR.Start < jR.Start)
                    {
                        next = iR;
                        i++;
                    }
                    else
                    {
                        next = jR;
                        j++;
                    }
                }
                else if (i < iCount)
                {
                    next = _intervals[i];
                    i++;
                }
                else
                {
                    next = intv._intervals[j];
                    j++;
                }

                if (next.Start > intEnd)
                {
                    union.Add(new DateInterval(intStart, intEnd));
                    intStart = next.Start;
                }
                else
                {
                    intStart = Min(next.Start, intStart);
                }

                if (i + j > 1)
                {
                    intEnd = Max(intEnd, next.End);
                }
                else
                {
                    // First step.
                    intEnd = next.End;
                }
            }
            if (intStart < intEnd)
            {
                union.Add(new DateInterval(intStart, intEnd));
            }
            _intervals = union;
        }

        private static DateTime Max(DateTime start1, DateTime start2)
        {
            return new DateTime(Math.Max(start1.Ticks, start2.Ticks));
        }

        private static DateTime Min(DateTime end1, DateTime end2)
        {
            return new DateTime(Math.Min(end1.Ticks, end2.Ticks));
        }

        public override string ToString()
        {
            return string.Join("#", _intervals.Select(i => i.ToString()));
        }
    }
}