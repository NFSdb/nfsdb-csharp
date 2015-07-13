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
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class ExpressionEvaluatorVisitor<T> 
    {
        private readonly IJournal<T> _journal;
        private readonly IReadTransactionContext _tx;

        internal ExpressionEvaluatorVisitor(IJournal<T> journal, IReadTransactionContext tx)
        {
            _journal = journal;
            _tx = tx;
        }

        public ResultSetBuilder<T> Visit(Expression exp)
        {
            if (exp == null)
                return null;

            switch (exp.NodeType)
            {
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                case ExpressionType.Not:
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.ArrayLength:
                case ExpressionType.Quote:
                case ExpressionType.TypeAs:
                case ExpressionType.UnaryPlus:
                    return VisitUnary((UnaryExpression) exp);
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.Coalesce:
                case ExpressionType.ArrayIndex:
                case ExpressionType.RightShift:
                case ExpressionType.LeftShift:
                case ExpressionType.ExclusiveOr:
                case ExpressionType.Power:
                    return VisitBinary((BinaryExpression) exp);
                case ExpressionType.Constant:
                    return VisitConstant((ConstantExpression) exp);
            }

            switch ((EJournalExpressionType)exp.NodeType)
            {
                case EJournalExpressionType.Contains:
                    return VisitContains((SymbolContainsExpression)exp);
                case EJournalExpressionType.Single:
                    return VisitCall((SingleItemExpression) exp);
                case EJournalExpressionType.Reverse:
                    return VisitCall((OrderExpression)exp);

                default:
                    throw new NFSdbQuaryableNotSupportedException(
                        "Expression {0} cannot be bound to Journal operation.", exp);
            }
        }

        private ResultSetBuilder<T> VisitCall(OrderExpression m)
        {
            var result = new ResultSetBuilder<T>(_journal, _tx);
            switch (m.Operation)
            {
                case EJournalExpressionType.Reverse:
                    result.Reverse(Visit(m.Body));
                    break;

                default:
                    throw new NFSdbQuaryableNotSupportedException(
                        "Expression call {0} cannot be bound to Journal operation.", m);
            }

            return result;
        }

        private ResultSetBuilder<T> VisitCall(SingleItemExpression m)
        {
            var result = new ResultSetBuilder<T>(_journal, _tx);
            switch (m.Operation)
            {
                case EJournalExpressionType.Single:
                    result.TakeSingle(Visit(m.Body));
                    break;

                default:
                    throw new NFSdbQuaryableNotSupportedException(
                        "Expression call {0} cannot be bound to Journal operation.", m);
            }

            return result;
        }

        private ResultSetBuilder<T> VisitConstant(ConstantExpression exp)
        {
            if (exp.Value is IQueryable<T>)
            {
                return new ResultSetBuilder<T>(_journal, _tx);
            }
            throw new NFSdbQuaryableNotSupportedException(
                "Expression {0} cannot be bound to Journal operation.", exp);
        }

        private ResultSetBuilder<T> VisitContains(SymbolContainsExpression exp)
        {
            if (exp.Match.NodeType == ExpressionType.MemberAccess)
            {
                var memberName = ExHelper.GetMemberName((MemberExpression)exp.Match, typeof(T));
                var result = new ResultSetBuilder<T>(_journal, _tx);
                result.IndexCollectionScan(memberName, exp.Values);

                return result;
            }
            throw new NFSdbQuaryableNotSupportedException(
                "Contains can only be bound for expression like List<string>.Contains(symbol_column))." +
                " Unable to bind {0}",
                exp);
        }

        private ResultSetBuilder<T> VisitBinary(BinaryExpression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Equal:
                    return EvaluateEquals(expression);

                case ExpressionType.Or:
                case ExpressionType.And:
                case ExpressionType.OrElse:
                case ExpressionType.AndAlso:
                    return EvaluateLogical(expression);

                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                    return EvaluateCompare(expression);

                default:
                    throw new NFSdbQuaryableNotSupportedException(
                        "Binary operation {0} cannot be bound to Journal operation", 
                        expression.NodeType);
            }
        }

        private ResultSetBuilder<T> EvaluateCompare(BinaryExpression expression)
        {
            var memberName = ExHelper.GetMemberName(expression, typeof(T));
            var literal = ExHelper.LiteralName(expression, typeof(T));
            if (GetTimestamp(_journal.Metadata)  != null &&
                GetTimestamp(_journal.Metadata).PropertyName == memberName)
            {
                if (literal is long || literal is DateTime)
                {
                    DateInterval filterInterval;
                    switch (expression.NodeType)
                    {
                        case ExpressionType.GreaterThan:
                            var timestamp = literal is long
                                ? DateUtils.UnixTimestampToDateTime((long) literal + 1)
                                : ((DateTime) literal).AddTicks(1);
                            filterInterval = DateInterval.From(timestamp);
                            break;

                        case ExpressionType.GreaterThanOrEqual:
                            timestamp = literal is long
                                ? DateUtils.UnixTimestampToDateTime((long) literal)
                                : (DateTime) literal;
                            filterInterval = DateInterval.From(timestamp);
                            break;

                        case ExpressionType.LessThan:
                            timestamp = literal is long
                                ? DateUtils.UnixTimestampToDateTime((long) literal)
                                : (DateTime) literal;
                            filterInterval = DateInterval.To(timestamp);
                            break;

                        case ExpressionType.LessThanOrEqual:
                            timestamp = literal is long
                                ? DateUtils.UnixTimestampToDateTime((long)literal + 1)
                                : ((DateTime)literal).AddTicks(1);
                            filterInterval = DateInterval.To(timestamp);
                            break;
                        default:
                            throw new NFSdbQuaryableNotSupportedException(
                                "Timestamp column operation {0} is not supported. Supported operations are <, >, <=, >=",
                                expression.NodeType);
                    }

                    var result = new ResultSetBuilder<T>(_journal, _tx);
                    result.TimestampInterval(filterInterval);
                    return result;
                }
            }
            throw new NFSdbQuaryableNotSupportedException(
                      "Comparison is supported for timestamp column only. Unable to bind {0} to journal query operation", expression);
        }

        private static ColumnMetadata GetTimestamp(IJournalMetadata<T> metadata)
        {
            if (!metadata.TimestampFieldID.HasValue)
            {
                return null;
            }
            return metadata.GetColumnById(metadata.TimestampFieldID.Value);
        }

        private ResultSetBuilder<T> EvaluateLogical(BinaryExpression expression)
        {
            var left = Visit(expression.Left);
            var right = Visit(expression.Right);

            var result = new ResultSetBuilder<T>(_journal, _tx);
            var operation = expression.NodeType;
            if (operation == ExpressionType.AndAlso)
            {
                operation = ExpressionType.And;
            }

            if (operation == ExpressionType.OrElse)
            {
                operation = ExpressionType.Or;
            }

            result.Logical(left, right, operation);

            return result;
        }

        private ResultSetBuilder<T> EvaluateEquals(BinaryExpression expression)
        {
            if (expression.NodeType == ExpressionType.Equal
                && (
                    (expression.Left.NodeType == ExpressionType.MemberAccess
                     && expression.Right.NodeType == ExpressionType.Constant)
                    || (expression.Left.NodeType == ExpressionType.Constant
                        && expression.Right.NodeType == ExpressionType.MemberAccess)
                    )
                )
            {
                var memberName = ExHelper.GetMemberName(expression, typeof (T));
                var literal = ExHelper.LiteralName(expression, typeof (T));
                if (literal is string)
                {
                    var result = new ResultSetBuilder<T>(_journal, _tx);
                    result.IndexScan(memberName, (string) literal);
                    return result;
                }


                if (literal is long || literal is DateTime)
                {

                    if (GetTimestamp(_journal.Metadata) != null &&
                        GetTimestamp(_journal.Metadata).PropertyName == memberName)
                    {
                        DateInterval filterInterval;
                        if (literal is long)
                        {
                            var timestamp = (long) literal;
                            filterInterval = new DateInterval(DateUtils.UnixTimestampToDateTime(timestamp),
                                DateUtils.UnixTimestampToDateTime(timestamp + 1));
                        }
                        else
                        {
                            var timestamp = (DateTime) literal;
                            filterInterval = new DateInterval(timestamp,
                                new DateTime(timestamp.Ticks + 1, timestamp.Kind));
                        }

                        var result = new ResultSetBuilder<T>(_journal, _tx);
                        result.TimestampInterval(filterInterval);
                        return result;
                    }
                }
            }
            throw new NotSupportedException(
                string.Format("Unable to translate expression {0} to journal operation", expression));
        }

        private ResultSetBuilder<T> VisitUnary(UnaryExpression exp)
        {
            throw new NotSupportedException();
        }
    }
}