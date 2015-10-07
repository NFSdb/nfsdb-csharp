using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class BinaryColumnValueComparer : IComparer<long>
    {
        private readonly IReadTransactionContext _tx;
        private readonly bool _asc;
        private readonly int _filedId;

        public BinaryColumnValueComparer(int fieldId, IReadTransactionContext tx, bool asc)
        {
            _filedId = fieldId;
            _tx = tx;
            _asc = asc;
        }

        public int Compare(long rowId1, long rowId2)
        {
            if (!_asc)
            {
                var tmp = rowId1;
                rowId1 = rowId2;
                rowId2 = tmp;
            }

            var part1 = RowIDUtil.ToPartitionIndex(rowId1);
            var part2 = RowIDUtil.ToPartitionIndex(rowId2);
            var lrow1 = RowIDUtil.ToLocalRowID(rowId1);
            var lrow2 = RowIDUtil.ToLocalRowID(rowId2);

            var col1 = _tx.Read(part1).ReadColumn(_filedId);
            var col2 = (part1 == part2) ? col1 : _tx.Read(part2).ReadColumn(_filedId);

            return Compare(((ITypedColumn<byte[]>) col1).Get(lrow1, _tx.ReadCache),
                ((ITypedColumn<byte[]>) col2).Get(lrow2, _tx.ReadCache));
        }

        public int Compare(byte[] rowId1, byte[] rowId2)
        {
            if (rowId1 == rowId2) return 0;
            if (rowId1 == null) return -1;
            if (rowId2 == null) return 1;

            int minLen = Math.Min(rowId1.Length, rowId2.Length);
            for (int i = 0; i < minLen; i++)
            {
                if (rowId1[i] < rowId2[i]) return -1;
                if (rowId1[i] > rowId2[i]) return 1;
            }

            return rowId1.Length.CompareTo(rowId2.Length);
        }
    }
}