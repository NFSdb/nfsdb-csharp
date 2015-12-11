using System.Collections.Generic;
using Apaf.NFSdb.Core.Queries.Queryable.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public struct QueryRowsResult
    {
        public readonly EJournalExpressionType PostExpression;
        public readonly IEnumerable<long> Rowids;
        public readonly long RowID;

        public QueryRowsResult(IEnumerable<long> rowids) : this()
        {
            Rowids = rowids;
        }

        public QueryRowsResult(long rowid) : this()
        {
            RowID = rowid;
        }

        public QueryRowsResult(IEnumerable<long> rowids, EJournalExpressionType postExpression)
            : this()
        {
            PostExpression = postExpression;
            Rowids = rowids;
        }

        public bool IsSingle
        {
            get { return Rowids == null; }
        }
    }
}