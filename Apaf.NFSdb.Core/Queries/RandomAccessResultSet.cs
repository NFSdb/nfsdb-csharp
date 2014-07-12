using System;
using System.Linq;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Queries
{
    public class RandomAccessResultSet<T> : ResultSet<T>
    {
        private readonly long[] _idArray;
        private readonly IReadContext _rx;

        public RandomAccessResultSet(IJournal<T> journal, long[] rowIDs, IReadContext rx) : base(journal, rx, rowIDs)
        {
            _idArray = rowIDs;
            _rx = rx;
            Length = rowIDs.Length;
        }

        public RandomAccessResultSet(Journal<T> journal, IReadContext rx) : base(journal, rx)
        {
            _rx = rx;
            _idArray = new long[0];
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
                    .GetColumnById(Journal.Metadata.TimestampFieldID.Value).FieldName;

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