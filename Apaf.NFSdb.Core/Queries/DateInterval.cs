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

namespace Apaf.NFSdb.Core.Queries
{
    public struct DateInterval
    {
        private static readonly DateInterval ANY = new DateInterval(DateTime.MinValue, DateTime.MaxValue);
        public DateTime Start { get; private set; }
        public DateTime End { get; private set; }

        public DateInterval(DateTime start, DateTime end) : this()
        {
            Start = start;
            End = end;
        }

        public static DateInterval Any
        {
            get { return ANY;  }
        }

        public static DateInterval From(DateTime start)
        {
            return new DateInterval
            {
                Start = start,
                End = DateTime.MaxValue
            };
        }

        public static DateInterval To(DateTime end)
        {
            return new DateInterval
            {
                End = end,
                Start = DateTime.MinValue
            };
        }

        public override string ToString()
        {
            return string.Format("{0},{1}", Start == DateTime.MinValue ? "-" : Start.ToString(), 
                End == DateTime.MaxValue ? "-": End.ToString());
        }
    }
}