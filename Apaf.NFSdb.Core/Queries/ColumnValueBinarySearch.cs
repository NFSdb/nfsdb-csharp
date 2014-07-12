using System;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Queries
{
    public static class ColumnValueBinarySearch
    {
        public static long LongBinarySerach(IFixedWidthColumn column, long value, long index, long count)
        {
            if (index < 0)
            {
                throw new ArgumentOutOfRangeException("index", "Must be non negative");
            }
            if (count <= 0)
            {
                throw new ArgumentOutOfRangeException("count", "Must be non negative");
            }
            var lo = index;
            var hi = count - 1;

            while (lo <= hi)
            {
                long i = (hi - lo) / 2 + lo;

                long c = column.GetInt64(i) - value;
                if (c == 0) return i;

                if (c < 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                }
            }
            return ~lo;
        }
    }
}