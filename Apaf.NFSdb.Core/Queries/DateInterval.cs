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
        public bool Equals(DateInterval other)
        {
            return Start.Equals(other.Start) && End.Equals(other.End);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Start.GetHashCode()*397) ^ End.GetHashCode();
            }
        }

        private static readonly DateInterval ANY = new DateInterval(DateTime.MinValue, DateTime.MaxValue);
        private static readonly DateInterval NONE = new DateInterval(DateTime.MinValue, DateTime.MinValue);

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

        public static DateInterval None
        {
            get { return NONE; }
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

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is DateInterval && Equals((DateInterval) obj);
        }
    }
}