using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable.PlanItem
{
    public class UnionPlanItem : IPlanItem
    {
        private readonly IPlanItem _left;
        private readonly IPlanItem _right;

        public UnionPlanItem(IPlanItem left, IPlanItem right)
        {
            _left = left;
            _right = right;
            Timestamps = new DateRange(_left.Timestamps);
            Timestamps.Union(_right.Timestamps);
        }

        public IEnumerable<long> Execute(IJournalCore journal, IReadTransactionContext tx)
        {
            return MergeDistinct(_left.Execute(journal, tx), _right.Execute(journal, tx));
        }

        public IPlanItem Left { get { return _left; } }
        public IPlanItem Right { get { return _right; } }

        private IEnumerable<long> MergeDistinct(IEnumerable<long> enum1, IEnumerable<long> enum2)
        {
            var e1 = enum1.GetEnumerator();
            var e2 = enum2.GetEnumerator();

            var e1Next = e1.MoveNext();
            var e2Next = e2.MoveNext();
            var lastRet = long.MinValue;

            var ei1 = e1Next ? e1.Current : long.MinValue;
            var ei2 = e2Next ? e2.Current : long.MinValue;

            while (e1Next || e2Next)
            {
                if (ei1 > ei2)
                {
                    if (ei1 != lastRet)
                    {
                        yield return ei1;
                        lastRet = ei1;
                    }
                    e1Next = e1.MoveNext();
                    ei1 = e1Next ? e1.Current : long.MinValue;
                }
                else
                {
                    if (ei2 != lastRet)
                    {
                        yield return ei2;
                        lastRet = ei2;
                    }
                    e2Next = e2.MoveNext();
                    ei2 = e2Next ? e2.Current : long.MinValue;
                }
            }
        }

        public long Cardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            return _left.Cardinality(journal, tx) + _right.Cardinality(journal, tx);
        }

        public DateRange Timestamps { get; private set; }

        public void Intersect(IPlanItem restriction)
        {
            Timestamps.Intersect(restriction.Timestamps);
            _left.Intersect(restriction);
            _right.Intersect(restriction);
        }
    }
}