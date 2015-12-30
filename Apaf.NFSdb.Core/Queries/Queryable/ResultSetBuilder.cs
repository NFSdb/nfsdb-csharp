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
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries.Queryable.Expressions;
using Apaf.NFSdb.Core.Queries.Queryable.PlanItem;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class ResultSetBuilder
    {
        private struct DirectExpression
        {
            public readonly EJournalExpressionType Expression;
            public readonly IColumnMetadata Column;
            public readonly int Count;

            public DirectExpression(EJournalExpressionType expression)
                : this()
            {
                Expression = expression;
            }

            public DirectExpression(EJournalExpressionType expression, IColumnMetadata column)
                : this()
            {
                Expression = expression;
                Column = column;
            }

            public DirectExpression(EJournalExpressionType expression, int count)
                : this()
            {
                Expression = expression;
                Count = count;
            }
        }

        private readonly IJournalCore _journal;
        private readonly IReadTransactionContext _tx;
        private IPlanItem _planHead;
        private readonly List<DirectExpression> _directExpressions = new List<DirectExpression>();
        private List<string> _columns;

        public ResultSetBuilder(IJournalCore journal, IReadTransactionContext tx)
        {
            _journal = journal;
            _tx = tx;
            _planHead = new TimestampRangePlanItem(DateInterval.Any);
        }

        public IPlanItem PlanItem { get { return _planHead; }}

        public IList<string> Columns
        {
            get { return _columns; }
        }

        public QueryRowsResult Build()
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
                        if (Metadata.TimestampColumnID == tranform.Column.ColumnID)
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

        private QueryRowsResult BindPostResult(IEnumerable<long> rowIds)
        {
            var hadNonTimestampOrder = false;
            foreach (var tranform in _directExpressions)
            {
                switch (tranform.Expression)
                {
                    case EJournalExpressionType.Single:
                        return new QueryRowsResult(rowIds.Single());
                    case EJournalExpressionType.First:
                        return new QueryRowsResult(rowIds.First());
                    case EJournalExpressionType.Last:
                        return new QueryRowsResult(rowIds.Last());
                    case EJournalExpressionType.Reverse:
                        if (hadNonTimestampOrder)
                        {
                            rowIds = rowIds.Reverse();
                        }
                        break;
                    case EJournalExpressionType.LongCount:
                    case EJournalExpressionType.Count:
                        return new QueryRowsResult(rowIds, tranform.Expression);

                    case EJournalExpressionType.OrderByDescending:
                    case EJournalExpressionType.OrderBy:
                        if (tranform.Column.ColumnID != Metadata.TimestampColumnID)
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
                    case EJournalExpressionType.FirstOrDefault:
                        return new QueryRowsResult(rowIds.FirstOrDefault());
                    case EJournalExpressionType.LastOrDefault:
                        return new QueryRowsResult(rowIds.LastOrDefault());

                    default:
                        throw QueryExceptionExtensions.NotSupported(
                            "Expression {0} is not expected to be post operation", tranform.Expression);
                }
            }
            return new QueryRowsResult(rowIds);
        }

        private IEnumerable<long> BindOrderBy(IEnumerable<long> rowIds, DirectExpression tranform)
        {
            var allRows = rowIds.ToList();
            allRows.Sort(tranform.Column.GetColumnComparer(_tx, tranform.Expression == EJournalExpressionType.OrderBy));
            return allRows;
        }

        private IPlanItem OptimizePlan(IPlanItem planHead)
        {
            return planHead;
        }

        public void ColumnScan<T>(IColumnMetadata column, T literal)
        {
            var planItem = new RowScanPlanItem(_journal, _tx);
            planItem.AddContainsScan(column, literal);
            _planHead = planItem;
        }

        public void ColumnLambdaScan<T>(IColumnMetadata column, Func<T, bool> lambda)
        {
            var planItem = new RowScanPlanItem(_journal, _tx);
            planItem.AddLambdaScan(column, lambda);
            _planHead = planItem;
        }

        public void ColumnNullScan(IColumnMetadata column)
        {
            var planItem = new RowScanPlanItem(_journal, _tx);
            _planHead = planItem;
            if (!Metadata.IsNullColumnID.HasValue || !column.Nullable || column.NullIndex < 0)
            {
                MakeEmpty();
                return;
            }

            var isNullColumn = Metadata.GetColumnByID(Metadata.IsNullColumnID.Value);
            Func<ByteArray, bool> lambda = ba => ba.IsSet(column.NullIndex);
            planItem.AddLambdaScan(isNullColumn, lambda);
        }

        public void ColumnNotNullScan(IColumnMetadata column)
        {
            var planItem = new RowScanPlanItem(_journal, _tx);
            _planHead = planItem;

            if (!Metadata.IsNullColumnID.HasValue || !column.Nullable || column.NullIndex < 0)
            {
                return;
            }

            var isNullColumn = Metadata.GetColumnByID(Metadata.IsNullColumnID.Value);
            Func<ByteArray, bool> lambda = ba => !ba.IsSet(column.NullIndex);
            planItem.AddLambdaScan(isNullColumn, lambda);
        }

        private IJournalMetadata Metadata
        {
            get { return _journal.Metadata; }
        }

        public void ApplyLinq(EJournalExpressionType operation)
        {
            _directExpressions.Add(new DirectExpression(operation));
        }

        public void ApplyLinq(EJournalExpressionType operation, int count)
        {
            if (operation == EJournalExpressionType.Skip && count == 0) return;
            if (operation == EJournalExpressionType.Take && count == int.MaxValue) return;
            if (operation == EJournalExpressionType.Take && count == 0)
            {
                // No rows to be selected.
                MakeEmpty();
                return;
            }

            _directExpressions.Add(new DirectExpression(operation, count));
        }

        public void MakeEmpty()
        {
            _planHead.Intersect(new TimestampRangePlanItem(DateInterval.None));
        }

        public void ApplyMap(List<ColumnNameExpression> columns)
        {
            _columns = new List<string>(columns.Count);
            foreach (var columnExp in columns)
            {
                var col = Metadata.Columns.FirstOrDefault(c => string.Equals(c.PropertyName, columnExp.Name, StringComparison.OrdinalIgnoreCase));
                if (col == null)
                {
                    throw QueryExceptionExtensions.ExpressionNotSupported(
                        "Column [{0}] does not exists in journal " + Metadata.Name, columnExp);
                }
                _columns.Add(col.PropertyName);
            }
        }

        public void ApplyOrderBy(IColumnMetadata column, EJournalExpressionType expression)
        {
            _directExpressions.Add(new DirectExpression(expression, column));
        }

        public void Logical(ResultSetBuilder left, ResultSetBuilder right, ExpressionType op)
        {
            if (op == ExpressionType.And)
            {
                _planHead = OptimizeIntersect(left._planHead, right._planHead);
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
                    var rowScan1 = (RowScanPlanItem) left._planHead;
                    var rowScan2 = (RowScanPlanItem) right._planHead;
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

        private static IPlanItem OptimizeIntersect(IPlanItem left, IPlanItem right)
        {
            if (right is TimestampRangePlanItem)
            {
                left.Intersect(right);
                return left;
            }
            if (left is TimestampRangePlanItem)
            {
                right.Intersect(left);
                return right;
            }
            
            if (left is RowScanPlanItem && right is RowScanPlanItem)
            {
                var rowScan1 = (RowScanPlanItem) left;
                var rowScan2 = (RowScanPlanItem) right;
                if (rowScan1.TryIntersect(rowScan2))
                {
                    return left;
                }
            }
            return new IntersectPlanItem(left, right);
        }

        public void IndexCollectionScan(string memberName, IEnumerable values, Expression exp)
        {
            var p = new RowScanPlanItem(_journal, _tx);
            var column = Metadata.GetColumnByPropertyName(memberName);
            switch (column.DataType.ColumnType)
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
                case EFieldType.DateTimeEpochMs:
                    p.AddContainsScan(column, ToIList<DateTime>(values));
                    break;
                default:
                    throw QueryExceptionExtensions.ExpressionNotSupported(
                        string.Format("Column of type {0} cannot be bound to Contains expressions", column.DataType.ColumnType), exp);
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
            p.AddContainsScan(Metadata.GetColumnByPropertyName(memberName), values);
            _planHead = p;
        }

        public void TimestampInterval(DateInterval filterInterval)
        {
            _planHead = new TimestampRangePlanItem(filterInterval);
        }

        public void TakeLatestBy(string latestBySymbol)
        {
            var column = Metadata.GetColumnByPropertyName(latestBySymbol);

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

        private IPlanItem CreateLastestByIdPlanItem(IColumnMetadata column)
        {
            var rowScan = new RowScanPlanItem(_journal, _tx);
            rowScan.ApplyLastestByIdPlanItem(column);
            return rowScan;
        }

        private IPlanItem RebuildWithLatest(IPlanItem planHead, IColumnMetadata latestByColumn)
        {
            var column = Metadata.GetColumnByPropertyName(latestByColumn.PropertyName);
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
                var col = Metadata.GetColumnByPropertyName(latestBySymbol);
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

        public bool CanApplyFilter()
        {
            return !_directExpressions.Any(e => e.Expression == EJournalExpressionType.Count
                                               || e.Expression == EJournalExpressionType.First
                                               || e.Expression == EJournalExpressionType.Last
                                               || e.Expression == EJournalExpressionType.LongCount
                                               || e.Expression == EJournalExpressionType.Single
                                               || e.Expression == EJournalExpressionType.Skip
                                               || e.Expression == EJournalExpressionType.Take);
        }

        public void ApplyFilter(ResultSetBuilder filter)
        {
            _planHead = OptimizeIntersect(_planHead, filter._planHead);
        }
    }
}