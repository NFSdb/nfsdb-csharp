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