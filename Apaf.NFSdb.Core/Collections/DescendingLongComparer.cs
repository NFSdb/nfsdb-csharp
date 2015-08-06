using System.Collections.Generic;

namespace Apaf.NFSdb.Core.Collections
{
    public class DescendingLongComparer : IComparer<long>
    {
        public int Compare(long x, long y)
        {
            if (y < x) return -1;
            if (x > y) return 1;
            return 0;
        }
    }
}