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
using Apaf.NFSdb.Core.Column;
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
        private bool _reverse;

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
            var result = new ResultSet<T>(_planHead.Execute(_journal, _tx), _tx);

            if (_reverse)
            {
                result = result.Reverse();
            }

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

        public void IndexScan(string memberName, object literal)
        {
            var column = _journal.Metadata.GetColumnByPropertyName(memberName);
            switch (column.FieldType)
            {
                case EFieldType.Byte:
                    _planHead = new ColumnScanPlanItem<byte>(column, new[] { (byte)literal });
                    break;
                case EFieldType.Bool:
                    _planHead = new ColumnScanPlanItem<bool>(column, new[] { (bool)literal });
                    break;
                case EFieldType.Int16:
                    _planHead = new ColumnScanPlanItem<Int16>(column, new[] { (Int16)literal });
                    break;
                case EFieldType.Int32:
                    _planHead = new ColumnScanPlanItem<int>(column, new[] { (int)literal });
                    break;
                case EFieldType.Int64:
                    _planHead = new ColumnScanPlanItem<long>(column, new[] { (long)literal });
                    break;
                case EFieldType.Double:
                    _planHead = new ColumnScanPlanItem<double>(column, new[] { (double)literal });
                    break;
                case EFieldType.String:
                    _planHead = new ColumnScanPlanItem<string>(column, new[] { (string)literal });
                    break;
                case EFieldType.Symbol:
                    _planHead = new ColumnScanPlanItem<string>(column, new[] { (string)literal });
                    break;
                case EFieldType.Binary:
                    _planHead = new ColumnScanPlanItem<byte[]>(column, new[] { (byte[])literal });
                    break;
                case EFieldType.DateTime:
                case EFieldType.DateTimeEpochMilliseconds:
                    _planHead = new ColumnScanPlanItem<DateTime>(column, new[] { (DateTime)literal });
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
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

        public void IndexCollectionScan<TT>(string memberName, TT[] values)
        {
            _planHead = new ColumnScanPlanItem<TT>(_journal.Metadata.GetColumnByPropertyName(memberName), values);
        }

        public void TimestampInterval(DateInterval filterInterval)
        {
            _planHead = new TimestampRangePlanItem(filterInterval);
        }

        public void TakeLatestBy(string latestBySymbol)
        {
            var column = _journal.Metadata.GetColumnByPropertyName(latestBySymbol);

            if (_planHead == null)
            {
                _planHead = CreateLastestByIdPlanItem(column);
            }
            else if (_planHead is TimestampRangePlanItem)
            {
                var newHead = CreateLastestByIdPlanItem(column);
                newHead.Intersect(_planHead);
                _planHead = newHead;
            }
            else
            {
                if (!AddLatestToColumnScan(_planHead, latestBySymbol))
                {
                    var queryBySym = CreateLastestByIdPlanItem(column);
                    _planHead = new IntersectPlanItem(queryBySym, _planHead);
                }
                else
                {
                    _planHead = RebuildWithLatest(_planHead, latestBySymbol);
                }
            }
        }

        private IPlanItem CreateLastestByIdPlanItem(ColumnMetadata column)
        {
            switch (column.FieldType)
            {
                case EFieldType.Byte:
                    return new LastestByIdPlanItem<byte>(column);
                case EFieldType.Bool:
                    return new LastestByIdPlanItem<bool>(column);
                case EFieldType.Int16:
                    return new LastestByIdPlanItem<Int16>(column);
                case EFieldType.Int32:
                    return new LastestByIdPlanItem<int>(column);
                case EFieldType.Int64:
                    return new LastestByIdPlanItem<long>(column);
                case EFieldType.Double:
                    return new LastestByIdPlanItem<double>(column);
                case EFieldType.String:
                case EFieldType.Symbol:
                    return new LastestByIdPlanItem<string>(column);
                case EFieldType.Binary:
                    return new LastestByIdPlanItem<byte[]>(column);
                case EFieldType.DateTime:
                case EFieldType.DateTimeEpochMilliseconds:
                    return new LastestByIdPlanItem<DateTime>(column);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private IPlanItem RebuildWithLatest(IPlanItem planHead, string latestBySymbolName)
        {
            var column = _journal.Metadata.GetColumnByPropertyName(latestBySymbolName);
            var intersc = planHead as IntersectPlanItem;
            if (intersc != null)
            {
                if (AddLatestToColumnScan(intersc.Left, latestBySymbolName))
                {
                    return 
                        new IntersectPlanItem(
                            RebuildWithLatest(intersc.Left, latestBySymbolName), 
                            intersc.Right);
                }
                if (AddLatestToColumnScan(intersc.Right, latestBySymbolName))
                {
                    return new IntersectPlanItem(intersc.Left,
                        RebuildWithLatest(intersc.Right, latestBySymbolName));
                }
                throw new InvalidOperationException("One of the Intersect path " +
                                                    "supposed to be reduced with" +
                                                    " Latest by symbol plan");
            }

            var union = planHead as UnionPlanItem;
            if (union != null)
            {
                return new UnionPlanItem(
                    RebuildWithLatest(union.Left, latestBySymbolName),
                    RebuildWithLatest(union.Right, latestBySymbolName)); 
            }

            var scan = planHead as IColumnScanPlanItemCore;
            if (scan != null)
            {
                if (scan.SymbolName == latestBySymbolName)
                {
                    return scan.ToLastestByIdPlanItem();
                }
            }
            var timestmp = planHead as TimestampRangePlanItem;
            if (timestmp != null)
            {
                var newItem = CreateLastestByIdPlanItem(column);
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
            var scan = planHead as IColumnScanPlanItemCore;
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

        public void Reverse(ResultSetBuilder<T> visit)
        {
            _reverse = true;
            _planHead = visit._planHead;
        }
    }
}