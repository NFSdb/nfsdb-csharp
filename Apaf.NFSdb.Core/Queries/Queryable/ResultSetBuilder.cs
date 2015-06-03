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
using System.Linq.Expressions;
using Apaf.NFSdb.Core.Queries.Queryable.PlanItem;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class ResultSetBuilder<T>
    {
        private readonly IJournal<T> _journal;
        private readonly IReadTransactionContext _tx;
        private IPlanItem _planHead;
        private bool _takeSingle;

        public ResultSetBuilder(IJournal<T> journal, IReadTransactionContext tx)
        {
            _journal = journal;
            _tx = tx;
        }

        public IPlanItem PlanItem { get { return _planHead; }}

        public object Build()
        {
            if (_planHead != null)
            {
                _planHead = OptimizePlan(_planHead);
            }
            else
            {
                _planHead = new TimestampRangePlanItem(DateInterval.Any);
            }
            var result = new ResultSet<T>(_journal, _tx.ReadCache,
                _planHead.Execute(_journal, _tx), _tx.Partitions);

            // Bind call.
            if (_takeSingle)
            {
                return result.Single();
            }

            return result;
        }

        private IPlanItem OptimizePlan(IPlanItem planHead)
        {
            return planHead;
        }

        public void IndexScan(string memberName, string literal)
        {
            _planHead = new ColumnScanPlanItem(memberName, new[] {literal});
        }

        public void Logical(ResultSetBuilder<T> left, ResultSetBuilder<T> right, ExpressionType op)
        {
            if (op == ExpressionType.And)
            {
                if (left._planHead is TimestampRangePlanItem
                    && right._planHead is TimestampRangePlanItem)
                {
                    _planHead = left._planHead;
                    _planHead.Intersect(right._planHead);
                }
                else if (right._planHead is TimestampRangePlanItem)
                {
                    _planHead = left._planHead;
                    _planHead.Intersect(right._planHead);
                }
                else if (left._planHead is TimestampRangePlanItem)
                {
                    _planHead = right._planHead;
                    _planHead.Intersect(left._planHead);
                }
                else
                {
                    _planHead = new IntersectPlanItem(left._planHead, right._planHead);
                }
            }
            else if (op == ExpressionType.Or)
            {
                if (left._planHead is TimestampRangePlanItem
                    && right._planHead is TimestampRangePlanItem)
                {
                    _planHead = left._planHead;
                    _planHead.Timestamps.Union(right._planHead.Timestamps);
                }
                else
                {
                    _planHead = new UnionPlanItem(left._planHead, right._planHead);
                }
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void IndexCollectionScan(string memberName, string[] values)
        {
            _planHead = new ColumnScanPlanItem(memberName, values);
        }

        public void TimestampInterval(DateInterval filterInterval)
        {
            _planHead = new TimestampRangePlanItem(filterInterval);
        }

        public void TakeLatestBy(string latestBySymbol)
        {
            if (_planHead == null)
            {
                _planHead = new LastestByIdPlanItem(latestBySymbol);
            }
            else if (_planHead is TimestampRangePlanItem)
            {
                var newHead = new LastestByIdPlanItem(latestBySymbol);
                newHead.Intersect(_planHead);
                _planHead = newHead;
            }
            else
            {
                if (!AddLatestToColumnScan(_planHead, latestBySymbol))
                {
                    var queryBySym = new LastestByIdPlanItem(latestBySymbol);
                    _planHead = new IntersectPlanItem(queryBySym, _planHead);
                }
                else
                {
                    _planHead = RebuildWithLatest(_planHead, latestBySymbol);
                }
            }
        }

        private IPlanItem RebuildWithLatest(IPlanItem planHead, string latestBySymbol)
        {
            var intersc = planHead as IntersectPlanItem;
            if (intersc != null)
            {
                if (AddLatestToColumnScan(intersc.Left, latestBySymbol))
                {
                    return 
                        new IntersectPlanItem(
                            RebuildWithLatest(intersc.Left, latestBySymbol), 
                            intersc.Right);
                }
                if (AddLatestToColumnScan(intersc.Right, latestBySymbol))
                {
                    return new IntersectPlanItem(intersc.Left,
                        RebuildWithLatest(intersc.Right, latestBySymbol));
                }
                throw new InvalidOperationException("One of the Intersect path " +
                                                    "supposed to be reduced with" +
                                                    " Latest by symbol plan");
            }

            var union = planHead as UnionPlanItem;
            if (union != null)
            {
                return new UnionPlanItem(
                    RebuildWithLatest(union.Left, latestBySymbol),
                    RebuildWithLatest(union.Right, latestBySymbol)); 
            }

            var scan = planHead as ColumnScanPlanItem;
            if (scan != null)
            {
                if (scan.SymbolName == latestBySymbol)
                {
                    return new LastestByIdPlanItem(latestBySymbol, scan.Literals);
                }
            }
            var timestmp = planHead as TimestampRangePlanItem;
            if (timestmp != null)
            {
                var newItem = new LastestByIdPlanItem(latestBySymbol);
                newItem.Timestamps.Intersect(timestmp.Timestamps);
                return newItem;
            }

            throw new NotSupportedException("Plan type is not supported " + planHead.GetType());
        
        }

        private bool AddLatestToColumnScan(IPlanItem planHead, string latestBySymbol)
        {
            var intersc = planHead as IntersectPlanItem;
            if (intersc != null)
            {
                return AddLatestToColumnScan(intersc.Left, latestBySymbol)
                       || AddLatestToColumnScan(intersc.Right, latestBySymbol);
            }
            var union = planHead as UnionPlanItem;
            if (union != null)
            {
                return AddLatestToColumnScan(union.Left, latestBySymbol)
                       && AddLatestToColumnScan(union.Right, latestBySymbol);
            }
            var scan = planHead as ColumnScanPlanItem;
            if (scan != null)
            {
                return scan.SymbolName == latestBySymbol;
            }
            var timestmp = planHead as TimestampRangePlanItem;
            if (timestmp != null)
            {
                return true;
            }

            throw new NotSupportedException("Plan type is not supported " + planHead.GetType());
        }

        public void TakeSingle(ResultSetBuilder<T> other)
        {
            _takeSingle = true;
            _planHead = other._planHead;
        }
    }
}