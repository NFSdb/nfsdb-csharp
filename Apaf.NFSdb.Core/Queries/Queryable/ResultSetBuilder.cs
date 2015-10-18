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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Queries.Queryable.Expressions;
using Apaf.NFSdb.Core.Queries.Queryable.PlanItem;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class ResultSetBuilder<T>
    {
        private class DirectExpression
        {
            public readonly EJournalExpressionType Expression;
            public readonly string Property;
            public readonly int Count;

            public DirectExpression(EJournalExpressionType expression)
            {
                Expression = expression;
            }

            public DirectExpression(EJournalExpressionType expression, string property)
            {
                Expression = expression;
                Property = property;
            }

            public DirectExpression(EJournalExpressionType expression, int count)
            {
                Expression = expression;
                Count = count;
            }
        }

        private readonly IJournal<T> _journal;
        private readonly IReadTransactionContext _tx;
        private IPlanItem _planHead;
        private readonly List<DirectExpression> _directExpressions = new List<DirectExpression>();

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
            IEnumerable<long> rowIDs = _planHead.Execute(_journal, _tx, GetTimestampOrder());
            return BindPostResult(rowIDs);
        }

        private ERowIDSortDirection GetTimestampOrder()
        {
            var order = ERowIDSortDirection.Desc;
            string timestampProperty = null;
            if (_journal.Metadata.TimestampFieldID != null)
            {
                timestampProperty = _journal.Metadata.GetColumnById(_journal.Metadata.TimestampFieldID.Value).PropertyName;
            }

            bool hadNonTimestampOrder = false;
            foreach (var tranform in _directExpressions)
            {
                switch (tranform.Expression)
                {
                    case EJournalExpressionType.Reverse:
                        if (!hadNonTimestampOrder)
                        {
                            order = order == ERowIDSortDirection.Asc
                                ? ERowIDSortDirection.Desc
                                : ERowIDSortDirection.Asc;
                        }
                        break;

                    case EJournalExpressionType.LongCount:
                    case EJournalExpressionType.Count:
                        if (!hadNonTimestampOrder)
                        {
                            // No benefit of pre-sorting. Use natural order.
                            return ERowIDSortDirection.Desc;
                        }
                        break;

                    case EJournalExpressionType.OrderBy:
                    case EJournalExpressionType.OrderByDescending:
                        if (tranform.Property == timestampProperty)
                        {
                            order = tranform.Expression == EJournalExpressionType.OrderBy
                                ? ERowIDSortDirection.Asc
                                : ERowIDSortDirection.Desc;
                        }
                        else
                        {
                            hadNonTimestampOrder = true;
                        }
                        break;
                }
            }
            return order;
        }

        private object BindPostResult(IEnumerable<long> rowIds)
        {
            string timestampProperty = null;
            if (_journal.Metadata.TimestampFieldID != null)
            {
                timestampProperty = _journal.Metadata.GetColumnById(_journal.Metadata.TimestampFieldID.Value).PropertyName;
            }

            bool hadNonTimestampOrder = false;

            foreach (var tranform in _directExpressions)
            {
                switch (tranform.Expression)
                {
                    case EJournalExpressionType.Single:
                        return new ResultSet<T>(rowIds, _tx).Single();
                    case EJournalExpressionType.First:
                        return new ResultSet<T>(rowIds, _tx).First();
                    case EJournalExpressionType.Last:
                        return new ResultSet<T>(rowIds, _tx).Last();
                    case EJournalExpressionType.Reverse:
                        if (hadNonTimestampOrder)
                        {
                            rowIds = rowIds.Reverse();
                        }
                        break;
                    case EJournalExpressionType.LongCount:
                        return rowIds.LongCount();
                    case EJournalExpressionType.Count:
                        return rowIds.Count();

                    case EJournalExpressionType.OrderByDescending:
                    case EJournalExpressionType.OrderBy:
                        if (tranform.Property != timestampProperty)
                        {
                            hadNonTimestampOrder = true;
                            rowIds = BindOrderBy(rowIds, tranform);
                        }
                        break;

                    case EJournalExpressionType.Take:
                        rowIds = rowIds.Take(tranform.Count);
                        break;
                    case EJournalExpressionType.Skip:
                        rowIds = rowIds.Skip(tranform.Count);
                        break;

                    default:
                        throw new NFSdbQueryableNotSupportedException(
                            "Expression {0} is not expected to be post operation", tranform.Expression);
                }
            }
            return new ResultSet<T>(rowIds, _tx);
        }

        private IEnumerable<long> BindOrderBy(IEnumerable<long> rowIds, DirectExpression tranform)
        {
            var column = _journal.MetadataCore.GetColumnByPropertyName(tranform.Property);
            var allRows = rowIds.ToList();
            allRows.Sort(column.GetComparer(_tx, tranform.Expression == EJournalExpressionType.OrderBy));
            return allRows;
        }

        private IPlanItem OptimizePlan(IPlanItem planHead)
        {
            return planHead;
        }

        public void ColumnScan(string memberName, object literal)
        {
            var column = _journal.Metadata.GetColumnByPropertyName(memberName);
            var planItem = new RowScanPlanItem(_journal, _tx);
            _planHead = planItem;
            switch (column.FieldType)
            {
                case EFieldType.Byte:
                    planItem.AddContainsScan(column, (byte)literal);
                    break;
                case EFieldType.Bool:
                    planItem.AddContainsScan(column, (bool)literal);
                    break;
                case EFieldType.Int16:
                    planItem.AddContainsScan(column, (Int16)literal);
                    break;
                case EFieldType.Int32:
                    planItem.AddContainsScan(column, (int)literal);
                    break;
                case EFieldType.Int64:
                    planItem.AddContainsScan(column, (long)literal);
                    break;
                case EFieldType.Double:
                    planItem.AddContainsScan(column, (double)literal);
                    break;
                case EFieldType.Symbol:
                case EFieldType.String:
                    planItem.AddContainsScan(column, (string)literal);
                    break;
                case EFieldType.DateTime:
                case EFieldType.DateTimeEpochMilliseconds:
                    planItem.AddContainsScan(column, (DateTime)literal);
                    break;
                default:
                    throw new NFSdbQueryableNotSupportedException();
            }
        }

        public void ApplyLinq(EJournalExpressionType operation)
        {
            _directExpressions.Add(new DirectExpression(operation));
        }

        public void ApplyLinq(EJournalExpressionType operation, int count)
        {
            _directExpressions.Add(new DirectExpression(operation, count));
        }

        public void ApplyOrderBy(string member, EJournalExpressionType expression)
        {
            _directExpressions.Add(new DirectExpression(expression, member));
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
                else if (left._planHead is RowScanPlanItem && right._planHead is RowScanPlanItem)
                {
                    var rowScan1 = (RowScanPlanItem) left._planHead;
                    var rowScan2 = (RowScanPlanItem)right._planHead;
                    if (rowScan1.TryIntersect(rowScan2))
                    {
                        _planHead = left.PlanItem;
                    }
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
                    return;
                }
                if (left._planHead is RowScanPlanItem && right._planHead is RowScanPlanItem)
                {
                    var rowScan1 = (RowScanPlanItem)left._planHead;
                    var rowScan2 = (RowScanPlanItem)right._planHead;
                    if (rowScan1.TryUnion(rowScan2))
                    {
                        _planHead = left.PlanItem;
                        return;
                    }
                }
                _planHead = new UnionPlanItem(left._planHead, right._planHead);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void IndexCollectionScan(string memberName, IEnumerable values)
        {
            var p = new RowScanPlanItem(_journal, _tx);
            var column = _journal.Metadata.GetColumnByPropertyName(memberName);
            switch (column.FieldType)
            {
                case EFieldType.Byte:
                    p.AddContainsScan(column, ToIList<byte>(values));
                    break;
                case EFieldType.Bool:
                    p.AddContainsScan(column, ToIList<bool>(values));
                    break;
                case EFieldType.Int16:
                    p.AddContainsScan(column, ToIList<short>(values));
                    break;
                case EFieldType.Int32:
                    p.AddContainsScan(column, ToIList<int>(values));
                    break;
                case EFieldType.Int64:
                    p.AddContainsScan(column, ToIList<long>(values));
                    break;
                case EFieldType.Double:
                    p.AddContainsScan(column, ToIList<double>(values));
                    break;
                case EFieldType.Symbol:
                case EFieldType.String:
                    p.AddContainsScan(column, ToIList<string>(values));
                    break;
                case EFieldType.DateTime:
                case EFieldType.DateTimeEpochMilliseconds:
                    p.AddContainsScan(column, ToIList<DateTime>(values));
                    break;
                default:
                    throw new NFSdbQueryableNotSupportedException("Column of type {1} cannot be bound to Contains expressions", column.FieldType);
            }
            _planHead = p;
        }

        private IList<TT> ToIList<TT>(IEnumerable values)
        {
            var dir = values as IList<TT>;
            if (dir != null) return dir;
            return values.Cast<TT>().ToList();
        }

        public void IndexCollectionScan<TT>(string memberName, TT[] values)
        {
            var p = new RowScanPlanItem(_journal, _tx);
            p.AddContainsScan(_journal.Metadata.GetColumnByPropertyName(memberName), values);
            _planHead = p;
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
                    _planHead = RebuildWithLatest(_planHead, column);
                }
            }
        }

        private IPlanItem CreateLastestByIdPlanItem(ColumnMetadata column)
        {
            var rowScan = new RowScanPlanItem(_journal, _tx);
            rowScan.ApplyLastestByIdPlanItem(column);
            return rowScan;
        }

        private IPlanItem RebuildWithLatest(IPlanItem planHead, ColumnMetadata latestByColumn)
        {
            var column = _journal.Metadata.GetColumnByPropertyName(latestByColumn.PropertyName);
            var intersc = planHead as IntersectPlanItem;
            if (intersc != null)
            {
                if (AddLatestToColumnScan(intersc.Left, latestByColumn.PropertyName))
                {
                    return 
                        new IntersectPlanItem(
                            RebuildWithLatest(intersc.Left, latestByColumn), 
                            intersc.Right);
                }
                if (AddLatestToColumnScan(intersc.Right, latestByColumn.PropertyName))
                {
                    return new IntersectPlanItem(intersc.Left,
                        RebuildWithLatest(intersc.Right, latestByColumn));
                }
                throw new InvalidOperationException("One of the Intersect path " +
                                                    "supposed to be reduced with" +
                                                    " Latest by symbol plan");
            }

            var union = planHead as UnionPlanItem;
            if (union != null)
            {
                return new UnionPlanItem(
                    RebuildWithLatest(union.Left, latestByColumn),
                    RebuildWithLatest(union.Right, latestByColumn)); 
            }

            var scan = planHead as IColumnScanPlanItemCore;
            if (scan != null)
            {
                if (scan.CanTranformLastestByIdPlanItem(latestByColumn))
                {
                    scan.ApplyLastestByIdPlanItem(latestByColumn);
                    return planHead;
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
                var col = _journal.Metadata.GetColumnByPropertyName(latestBySymbol);
                if (scan.CanTranformLastestByIdPlanItem(col))
                {
                    return true;
                }
                return false;
            }

            var timestmp = planHead as TimestampRangePlanItem;
            if (timestmp != null)
            {
                return true;
            }

            throw new NotSupportedException("Plan type is not supported " + planHead.GetType());
        }

    }
}