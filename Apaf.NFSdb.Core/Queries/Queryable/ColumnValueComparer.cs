using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class ColumnValueComparer<TT> : IComparer<long> where TT : IComparable
    {
        private readonly IReadTransactionContext _tx;
        private readonly bool _asc;
        private readonly int _filedId;

        public ColumnValueComparer(int fieldId, IReadTransactionContext tx, bool asc)
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

            var val1 = ((ITypedColumn<TT>) col1).Get(lrow1, _tx.ReadCache);
            var val2 = ((ITypedColumn<TT>)col2).Get(lrow2, _tx.ReadCache);

            if (val1 != null)
            {
                return ((ITypedColumn<TT>)col1).Get(lrow1, _tx.ReadCache).CompareTo(
                    ((ITypedColumn<TT>)col2).Get(lrow2, _tx.ReadCache));
            }

            if (val2 == null)
            {
                return 0;
            }
            return -1;

        }
    }
}