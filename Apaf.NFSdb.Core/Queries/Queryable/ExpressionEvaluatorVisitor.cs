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
using Apaf.NFSdb.Core.Queries.Queryable.Expressions;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class ExpressionEvaluatorVisitor
    {
        private readonly IJournalCore _journal;
        private readonly IReadTransactionContext _tx;
        private readonly QlParameter[] _parameters;
        private readonly Type _itemType;

        internal ExpressionEvaluatorVisitor(IJournalCore journal, IReadTransactionContext tx, Type itemType)
        {
            _journal = journal;
            _tx = tx;
            _itemType = itemType;
        }

        internal ExpressionEvaluatorVisitor(IJournalCore journal, IReadTransactionContext tx, QlParameter[] parameters)
        {
            _journal = journal;
            _tx = tx;
            _parameters = parameters;
        }

        public ResultSetBuilder Visit(Expression exp)
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
                    return VisitBinary(exp);
                case ExpressionType.Constant:
                    return VisitConstant((ConstantExpression) exp);
            }

            switch ((EJournalExpressionType)exp.NodeType)
            {
                case EJournalExpressionType.Contains:
                    return VisitContains((SymbolContainsExpression)exp);
                case EJournalExpressionType.Single:
                case EJournalExpressionType.Count:
                case EJournalExpressionType.LongCount:
                case EJournalExpressionType.Reverse:
                case EJournalExpressionType.First:
                case EJournalExpressionType.FirstOrDefault:
                case EJournalExpressionType.Last:
                case EJournalExpressionType.LastOrDefault:
                    return VisitCall((PostResultExpression)exp);
                case EJournalExpressionType.OrderBy:
                case EJournalExpressionType.OrderByDescending:
                    return VisitOrderBy((OrderExpression) exp);
                case EJournalExpressionType.Take:
                case EJournalExpressionType.Skip:
                    return VisitCall((SliceExpression) exp);
                case EJournalExpressionType.LatestBy:
                    return VisitLatestBy((LatestBySymbolExpression)exp);
                case EJournalExpressionType.Intersect:
                    var intsct = (IntersectExpression) exp;
                    return VisitSet(intsct.Left, intsct.Right, ExpressionType.And);
                case EJournalExpressionType.Union:
                    var union = (UnionExpression)exp;
                    return VisitSet(union.Left, union.Right, ExpressionType.Or);
                case EJournalExpressionType.Filter:
                    return VisitFilter((FilterExpression) exp);
                case EJournalExpressionType.Journal:
                    return new ResultSetBuilder(_journal, _tx);;
                default:
                    throw QueryExceptionExtensions.ExpressionNotSupported(
                        "Expression {0} cannot be bound to Journal operation.", exp);
            }
        }


        private ResultSetBuilder VisitFilter(FilterExpression exp)
        {
            var source = Visit(exp.Source);
            if (!source.CanApplyFilter())
            {
                throw QueryExceptionExtensions.ExpressionNotSupported(
                        "Where cannot be applied after Take, Skip, Single, First or Count expressions.", exp);
            }
            var filter = Visit(exp.Filter);
            source.ApplyFilter(filter);
            return source;
        }

        private ResultSetBuilder VisitSet(Expression left, Expression right, ExpressionType operation)
        {
            var res = new ResultSetBuilder(_journal, _tx);
            res.Logical(Visit(left), Visit(right), operation);
            return res;
        }

        private ResultSetBuilder VisitLatestBy(LatestBySymbolExpression exp)
        {
            try
            {
                var result = exp.Body != null ? Visit(exp.Body) : new ResultSetBuilder(_journal, _tx);
                result.TakeLatestBy(exp.LatestBy);
                return result;
            }
            catch (NFSdbQueryableNotSupportedException ex)
            {
                throw QueryExceptionExtensions.ExpressionNotSupported(ex.Message, exp);
            }
        }

        private ResultSetBuilder VisitOrderBy(OrderExpression exp)
        {
            var result = Visit(exp.Body);
            var ex = exp.Predicate.Body as MemberExpression;
            if (ex != null)
            {
                var member = ExHelper.GetMemberName(ex, _itemType);
                result.ApplyOrderBy(member, (EJournalExpressionType)exp.NodeType);
                return result;
            }
            throw QueryExceptionExtensions.ExpressionNotSupported(
                string.Format("Order by is not supported on predicate {0}", exp.Predicate), exp);
        }

        private ResultSetBuilder VisitCall(PostResultExpression m)
        {
            var result = Visit(m.Body);
            result.ApplyLinq(m.Operation);
            return result;
        }

        private ResultSetBuilder VisitCall(SliceExpression m)
        {
            var result = Visit(m.Body);
            result.ApplyLinq(m.Operation, m.Count);
            return result;
        }

        private ResultSetBuilder VisitConstant(ConstantExpression exp)
        {
            var expType = exp.Value.GetType();
            if (expType.GetInterfaces().Any(x =>
                x.IsGenericType
                && x.GetGenericTypeDefinition() == typeof(IQueryable<>)
                && x.GetGenericArguments()[0] == _itemType))
            {
                return new ResultSetBuilder(_journal, _tx);
            }
            throw QueryExceptionExtensions.ExpressionNotSupported(
                "Expression {0} cannot be bound to Journal operation.", exp);
        }

        private ResultSetBuilder VisitContains(SymbolContainsExpression exp)
        {
            if (exp.Match.NodeType == ExpressionType.MemberAccess)
            {
                var memberName = ExHelper.GetMemberName((MemberExpression)exp.Match, _itemType);
                var result = new ResultSetBuilder(_journal, _tx);
                result.IndexCollectionScan(memberName, exp.Values, exp);

                return result;
            }
            throw QueryExceptionExtensions.ExpressionNotSupported(
                "Contains can only be bound for expression like List<string>.Contains(symbol_column))." +
                " Unable to bind {0}",
                exp);
        }

        private ResultSetBuilder VisitBinary(Expression expression)
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
                    throw QueryExceptionExtensions.NotSupported(
                        "Binary operation {0} cannot be bound to Journal operation", 
                        expression.NodeType);
            }
        }
        
        private ResultSetBuilder EvaluateCompare(Expression exp)
        {
            var memberName = ExHelper.GetMemberName(exp, _itemType);
            var literal = ExHelper.GetLiteralValue(exp, _parameters);

            if (GetTimestamp(_journal.MetadataCore) != null &&
                GetTimestamp(_journal.MetadataCore).PropertyName == memberName
                && (literal is long || literal is DateTime))
            {
                DateInterval filterInterval;
                var nodeType = exp.NodeType;
                if (exp.GetLeft().NodeType == ExpressionType.Constant)
                {
                    nodeType = InvertComarison(nodeType);
                }

                switch (nodeType)
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
                            ? DateUtils.UnixTimestampToDateTime((long) literal + 1)
                            : ((DateTime) literal).AddTicks(1);
                        filterInterval = DateInterval.To(timestamp);
                        break;
                    default:
                        throw QueryExceptionExtensions.ExpressionNotSupported(string.Format(
                            "Timestamp column operation {0} is not supported. Supported operations are <, >, <=, >=",
                            nodeType), exp);
                }

                var result = new ResultSetBuilder(_journal, _tx);
                result.TimestampInterval(filterInterval);
                return result;
            }
            throw QueryExceptionExtensions.ExpressionNotSupported(
                "Comparison is supported for timestamp column only. Unable to bind '{0} to journal query operation",
                exp);
        }

        private ExpressionType InvertComarison(ExpressionType nodeType)
        {
            switch (nodeType)
            {
                case ExpressionType.GreaterThan:
                    return ExpressionType.LessThan;

                case ExpressionType.GreaterThanOrEqual:
                    return ExpressionType.LessThanOrEqual;

                case ExpressionType.LessThan:
                    return ExpressionType.GreaterThan;

                case ExpressionType.LessThanOrEqual:
                    return ExpressionType.GreaterThanOrEqual;
            }
            return nodeType;
        }

        private static ColumnMetadata GetTimestamp(IJournalMetadataCore metadata)
        {
            if (!metadata.TimestampFieldID.HasValue)
            {
                return null;
            }
            return metadata.GetColumnById(metadata.TimestampFieldID.Value);
        }

        private ResultSetBuilder EvaluateLogical(Expression expression)
        {
            var left = Visit(expression.GetLeft());
            var right = Visit(expression.GetRight());

            var result = new ResultSetBuilder(_journal, _tx);
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

        private ResultSetBuilder EvaluateEquals(Expression expression)
        {
            if (expression.NodeType == ExpressionType.Equal)
            {
                var literal = ExHelper.GetLiteralValue(expression, _parameters);
                var memberName = ExHelper.GetMemberName(expression, _itemType);
                if (literal is long || literal is DateTime)
                {

                    if (GetTimestamp(_journal.MetadataCore) != null &&
                        GetTimestamp(_journal.MetadataCore).PropertyName == memberName)
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

                        var result = new ResultSetBuilder(_journal, _tx);
                        result.TimestampInterval(filterInterval);
                        return result;
                    }
                }
                else
                {
                    var result = new ResultSetBuilder(_journal, _tx);
                    try
                    {
                        result.ColumnScan(memberName, literal);
                    }
                    catch (NFSdbQueryableNotSupportedException ex)
                    {
                        throw QueryExceptionExtensions.ExpressionNotSupported(ex.Message, expression);
                    }
                    catch (InvalidCastException ex)
                    {
                        throw QueryExceptionExtensions.ExpressionNotSupported(ex.Message, expression);
                    }
                    return result;
                }
            }
            throw new NotSupportedException(
                string.Format("Unable to translate expression {0} to journal operation", expression));
        }

        private ResultSetBuilder VisitUnary(UnaryExpression exp)
        {
            throw new NotSupportedException();
        }
    }
}