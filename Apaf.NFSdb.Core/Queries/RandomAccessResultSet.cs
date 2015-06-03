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
using System.Linq;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Queries
{
    public class RandomAccessResultSet<T> : ResultSet<T>
    {
        private readonly long[] _idArray;
        private readonly IReadContext _rx;
        // ReSharper disable once StaticFieldInGenericType
        private static readonly long[] EMPTY_IDS = new long[0];

        public RandomAccessResultSet(IJournal<T> journal, long[] rowIDs, IReadContext rx, IPartitionTxSupport ptx)
            : base(journal, rx, rowIDs, ptx, rowIDs.Length)
        {
            _idArray = rowIDs;
            _rx = rx;
            Length = rowIDs.Length;
        }

        public RandomAccessResultSet(Journal<T> journal, IReadContext rx, IPartitionTxSupport ptx)
            : base(journal, rx, EMPTY_IDS, ptx, 0)
        {
            _rx = rx;
            _idArray = EMPTY_IDS;
            Length = 0;
        }

        public long[] GetRowIDs()
        {
            return _idArray;
        }

        public T Read(int rsIndex)
        {
            return Journal.Read(_idArray[rsIndex], _rx);
        }

        public long GetRowID(int index)
        {
            return _idArray[index];
        }

        public ResultSet<T> Sort(Order order, params string[] columnNames)
        {
            return Sort(order, ColumnIndices(columnNames));
        }

        public ResultSet<T> Sort(Order order, int[] columnIndices)
        {
            if (_idArray.Length > 0)
            {
                var comparer = Journal.GetRecordsComparer(columnIndices);
                Array.Sort(_idArray, comparer);
                // QuickSort(order, 0, _idArray.Length - 1, comparer);
            }
            return this;
        }

        public ResultSet<T> Sort()
        {
            if (Journal.Metadata.TimestampFieldID.HasValue)
            {
                var timestampName = Journal.Metadata
                    .GetColumnById(Journal.Metadata.TimestampFieldID.Value).FileName;

                Sort(Order.Asc, new[] { timestampName });
            }
            return this;
        }

        private int[] ColumnIndices(params string[] columnNames)
        {
            return columnNames.Select(Journal.Metadata.GetFieldID).ToArray();
        }

        //public void QuickSort(Order order, int start, int end, IComparer<long> comparer)
        //{
        //    int pIndex = start + (end - start) / 2;
        //    long pivot = _idArray[pIndex];

        //    int multiplier = 1;

        //    if (order == Order.Desc)
        //    {
        //        multiplier = -1;
        //    }

        //    int i = start;
        //    int j = end;

        //    while (i <= j)
        //    {

        //        while (multiplier * comparer.Compare(_idArray[i], pivot) < 0)
        //        {
        //            i++;
        //        }

        //        while (multiplier * comparer.Compare(pivot, _idArray[j]) < 0)
        //        {
        //            j--;
        //        }

        //        if (i <= j)
        //        {
        //            long temp = _idArray[i];
        //            _idArray[i] = _idArray[j];
        //            _idArray[j] = temp;
        //            i++;
        //            j--;
        //        }
        //    }
        //    if (start < j)
        //    {
        //        QuickSort(order, start, j, comparer);
        //    }

        //    if (i < end)
        //    {
        //        QuickSort(order, i, end, comparer);
        //    }
        //}

    }
}