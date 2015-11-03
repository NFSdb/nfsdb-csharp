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

        public IEnumerable<long> Execute(IJournalCore journal, IReadTransactionContext tx, ERowIDSortDirection sort)
        {
            return MergeDistinct(_left.Execute(journal, tx, sort), _right.Execute(journal, tx, sort), sort);
        }

        public IPlanItem Left { get { return _left; } }
        public IPlanItem Right { get { return _right; } }

        private IEnumerable<long> MergeDistinct(IEnumerable<long> enum1, IEnumerable<long> enum2, ERowIDSortDirection sort)
        {
            if (sort != ERowIDSortDirection.Asc)
            {
                return MergeDistinctDesc(enum1, enum2);
            }
            return MergeDistinctAsc(enum1, enum2);
        }

        private IEnumerable<long> MergeDistinctAsc(IEnumerable<long> enum1, IEnumerable<long> enum2)
        {
            var e1 = enum1.GetEnumerator();
            var e2 = enum2.GetEnumerator();

            var e1Next = e1.MoveNext();
            var e2Next = e2.MoveNext();
            var lastRet = long.MaxValue;

            var ei1 = e1Next ? e1.Current : long.MaxValue;
            var ei2 = e2Next ? e2.Current : long.MaxValue;

            while (e1Next || e2Next)
            {
                if (ei1 < ei2)
                {
                    if (ei1 != lastRet)
                    {
                        yield return ei1;
                        lastRet = ei1;
                    }
                    e1Next = e1.MoveNext();
                    ei1 = e1Next ? e1.Current : long.MaxValue;
                }
                else
                {
                    if (ei2 != lastRet)
                    {
                        yield return ei2;
                        lastRet = ei2;
                    }
                    e2Next = e2.MoveNext();
                    ei2 = e2Next ? e2.Current : long.MaxValue;
                }
            }
        }

        private static IEnumerable<long> MergeDistinctDesc(IEnumerable<long> enum1, IEnumerable<long> enum2)
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