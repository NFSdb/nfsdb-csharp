#region copyright
/*
 * Copyright (c) 2014. APAF (Alex Pelagenko).
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
using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable.PlanItem
{
    public class IntersectPlanItem : IPlanItem
    {
        private readonly IPlanItem _left;
        private readonly IPlanItem _right;

        public IntersectPlanItem(IPlanItem left, IPlanItem right)
        {
            _left = left;
            _right = right;
            Timestamps = new DateRange(left.Timestamps);
            Timestamps.Intersect(right.Timestamps);

            left.Intersect(right);
            right.Intersect(left);
        }

        public IEnumerable<long> Execute(IJournalCore journal, IReadTransactionContext tx)
        {
            // Timestamp restricted
            if (_left is TimestampRangePlanItem)
            {
                return _right.Execute(journal, tx);
            }
            
            if (_right is TimestampRangePlanItem)
            {
                return _left.Execute(journal, tx);
            }

            if (_left.Cardinality(journal, tx) < _right.Cardinality(journal, tx))
            {
                return Intersect(_left.Execute(journal, tx), _right.Execute(journal, tx));
            }
            return Intersect(_right.Execute(journal, tx), _left.Execute(journal, tx));
        }

        public long Cardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            return Math.Min(_left.Cardinality(journal, tx), _right.Cardinality(journal, tx)); 
        }

        public void Intersect(IPlanItem restriction)
        {
            Timestamps.Intersect(restriction.Timestamps);
            _left.Intersect(restriction);
            _right.Intersect(restriction);
        }

        public DateRange Timestamps { get; private set; }

        public IPlanItem Left { get { return _left; } }
        public IPlanItem Right { get { return _right; } }

        private IEnumerable<long> Intersect(IEnumerable<long> enum1, IEnumerable<long> enum2)
        {
            var e1 = enum1.GetEnumerator();
            var e2 = enum2.GetEnumerator();

            var e1Next = e1.MoveNext();
            var e2Next = e2.MoveNext();

            var ei1 = e1Next ? e1.Current : long.MinValue;
            var ei2 = e2Next ? e2.Current : long.MinValue;

            while (e1Next || e2Next)
            {
                if (ei1 == ei2)
                {
                    yield return ei1;
                }

                if (ei1 > ei2)
                {
                    e1Next = e1.MoveNext();
                    ei1 = e1Next ? e1.Current : long.MinValue;
                }
                else
                {
                    e2Next = e2.MoveNext();
                    ei2 = e2Next ? e2.Current : long.MinValue;
                }
            }

        }
    }
}