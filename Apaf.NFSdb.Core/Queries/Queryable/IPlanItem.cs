using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public interface IPlanItem
    {
        IEnumerable<long> Execute(IJournalCore journal, IReadTransactionContext tx);
        long Cardinality(IJournalCore journal, IReadTransactionContext tx);
        void Intersect(IPlanItem restriction);

        DateRange Timestamps { get; }
    }
}