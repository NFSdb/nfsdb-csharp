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