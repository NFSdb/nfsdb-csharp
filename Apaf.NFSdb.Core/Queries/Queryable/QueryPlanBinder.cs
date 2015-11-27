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
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using Apaf.NFSdb.Core.Queries.Queryable.Expressions;
using Apaf.NFSdb.Core.Tx;
using IQToolkit;
using IQToolkit.Data.Common;
using OrderExpression = Apaf.NFSdb.Core.Queries.Queryable.Expressions.OrderExpression;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class QueryPlanBinder<T> : DbExpressionVisitor
    {
        private readonly IJournalCore _journal;
        private readonly IReadTransactionContext _tx;

        public QueryPlanBinder(IJournalCore journal, IReadTransactionContext tx)
        {
            _journal = journal;
            _tx = tx;
        }

        public ResultSetBuilder Build(Expression ex)
        {
            var translatedTree = Visit(ex);
            var evalVis = new ExpressionEvaluatorVisitor(_journal, _tx, typeof(T));
            return evalVis.Visit(translatedTree);
        }

        protected override Expression Visit(Expression exp)
        {
            exp = PartialEvaluator.Eval(exp, CanBeEvaluatedLocally);
            Expression result = base.Visit(exp);

            if (result != null)
            {
                // bindings that expect projections should have called VisitSequence, the rest will probably get annoyed if
                // the projection does not have the expected type.
                Type expectedType = exp.Type;
                var projection = result as ProjectionExpression;
                if (projection != null && projection.Aggregator == null && 
                    !expectedType.IsAssignableFrom(projection.Type))
                {
                    LambdaExpression aggregator = 
                        Aggregator.GetAggregator(expectedType, projection.Type);

                    if (aggregator != null)
                    {
                        return new ProjectionExpression(projection.Select, 
                            projection.Projector, aggregator);
                    }
                }
            }

            return result;
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            if (m.Method.DeclaringType == typeof (System.Linq.Queryable)
                || (m.Method.DeclaringType == typeof(Enumerable))
                || (m.Method.DeclaringType != null && typeof(IEnumerable).IsAssignableFrom(m.Method.DeclaringType))
                || m.Method.DeclaringType == typeof (JournalQueriableExtensions))
            {
                switch (m.Method.Name)
                {
                    case "Union":
                        return BindUnion(m.Type, m.Arguments[0], m.Arguments[1]);

                    case "Where":
                        return BindWhere(m.Type, m.Arguments[0], GetLambda(m.Arguments[1]));

                    case "Single":
                        return BindPostResult(m.Type, m.Arguments, EJournalExpressionType.Single);

                    case "First":
                        return BindPostResult(m.Type, m.Arguments, EJournalExpressionType.First);

                    case "Last":
                        return BindPostResult(m.Type, m.Arguments, EJournalExpressionType.Last);

                    case "FirstOrDefault":
                        return BindPostResult(m.Type, m.Arguments, EJournalExpressionType.FirstOrDefault);

                    case "LastOrDefault":
                        return BindPostResult(m.Type, m.Arguments, EJournalExpressionType.LastOrDefault);

                    case "Reverse":
                        return BindReverse(m.Arguments[0]);

                    case "OrderBy":
                        return BindOrderBy(m.Arguments[0], GetLambda(m.Arguments[1]), EJournalExpressionType.OrderBy);

                    case "ThenBy":
                        return BindOrderBy(m.Arguments[0], GetLambda(m.Arguments[1]), EJournalExpressionType.OrderBy);

                    case "OrderByDescending":
                        return BindOrderBy(m.Arguments[0], GetLambda(m.Arguments[1]), EJournalExpressionType.OrderByDescending);

                    case "ThenByDescending":
                        return BindOrderBy(m.Arguments[0], GetLambda(m.Arguments[1]), EJournalExpressionType.OrderBy);

                    case "Count":
                        return new PostResultExpression(Visit(m.Arguments[0]), EJournalExpressionType.Count);

                    case "LongCount":
                        return new PostResultExpression(Visit(m.Arguments[0]), EJournalExpressionType.LongCount);

                    case "Take":
                        return new SliceExpression(Visit(m.Arguments[0]), EJournalExpressionType.Take, GetConstant<int>(m.Arguments[1]));

                    case "Skip":
                        return new SliceExpression(Visit(m.Arguments[0]), EJournalExpressionType.Skip, GetConstant<int>(m.Arguments[1]));

                    case "Contains":
                        if (m.Arguments.Count == 2)
                        {
                            return BindContains(m.Arguments[0], m.Arguments[1]);
                        }
                        if (m.Arguments.Count == 1)
                        {
                            return BindContains(m.Object, m.Arguments[0]);
                        }
                        break;

                    case "LatestBy":
                        if (m.Arguments.Count == 2)
                        {
                            return BindLatestBy(m.Arguments[0],  GetLambda(m.Arguments[1]));
                        }
                        break;
                }
            }


            return base.VisitMethodCall(m);
        }

        private Expression BindLatestBy(Expression ex1, LambdaExpression lambda)
        {
            var prop = lambda.Body as MemberExpression;
            if (prop != null)
            {
                return new LatestBySymbolExpression(prop.Member.Name, Visit(ex1));
            }
            throw QueryExceptionExtensions.NotSupported("LatestBy is only supported with property" +
                                                          " expression, but instead had '{0}'",
                lambda);
        }

        private Expression BindUnion(Type type, Expression expression1, Expression expression2)
        {
            if (typeof(IQueryable<T>).IsAssignableFrom(type))
            {
                var mc1 = expression1 as MethodCallExpression;
                var mc2 = expression2 as MethodCallExpression;
                if (mc1 != null && mc2 != null)
                {
                    if (IsQuery(mc1.Arguments[0]) && IsQuery(mc2.Arguments[0]))
                    {
                        return new UnionExpression(Visit(mc1), Visit(mc2));
                    }
                }
            }
            throw QueryExceptionExtensions.NotSupported("Union of 2 journal queriables supported. " +
                                                          "Attempted to join '{0}' with '{1}' instead.",
                expression1, expression2);
        }

        private TT GetConstant<TT>(Expression expression)
        {
            var c = expression as ConstantExpression;
            if (c != null)
            {
                if (c.Type == typeof (TT))
                {
                    return (TT)c.Value;
                }
            }
            throw QueryExceptionExtensions.NotSupported("Expected constant expression of type {0} but got {1}",
                typeof(T).Name, expression);
        }

        private Expression BindOrderBy(Expression source, LambdaExpression lambdaExpression, EJournalExpressionType direction)
        {
            return new OrderExpression(Visit(source), direction, lambdaExpression);
        }

        private Expression BindReverse(Expression expression)
        {
            return new OrderExpression(Visit(expression), EJournalExpressionType.Reverse);
        }

        private Expression BindContains(Expression source, Expression match)
        {
            var constSource = source as ConstantExpression;
            if (constSource != null && !IsQuery(constSource))
            {
                var stringEnum = constSource.Value as IEnumerable;
                if (stringEnum != null)
                {
                    match = Visit(match);
                    return new SymbolContainsExpression(match, Expression.Constant(stringEnum));
                }
            }

            throw QueryExceptionExtensions.NotSupported(
                "List.Contains, Array.Contains allowed only. Unable to execute Contains on {0}.",
                constSource);
        }

        private bool IsQuery(Expression expression)
        {
            Type elementType = TypeHelper.GetElementType(expression.Type);
            return elementType != null && typeof(IQueryable<>)
                .MakeGenericType(elementType).IsAssignableFrom(expression.Type);
        }

        private Expression BindWhere(Type resultType, Expression source, LambdaExpression predicate)
        {
            if (resultType != typeof (IQueryable<T>))
            {
                throw QueryExceptionExtensions.ExpressionNotSupported(
                    "Usage of expressions in select statment is not allowed."
                    + " Please convert to IEnumerable and perform select statement.", source);
            }
            var where = Visit(predicate.Body);
            if (source is ConstantExpression)
            {
                return where;
            }

            source = Visit(source);
            if (!(source is FilterExpression) && !(source is PostResultExpression))
            {
                return new IntersectExpression(source, where);
            }

            return new FilterExpression(where, source);
        }

        private Expression BindPostResult(Type resultType, ReadOnlyCollection<Expression> arguments, EJournalExpressionType expressionType)
        {
            if (resultType != typeof(T))
            {
                throw QueryExceptionExtensions.NotSupported("{0} operation canonly be bound to JournalQueryable of {1} but used on {2}",
                    expressionType, typeof(T).Name, resultType);
            }
            var source = arguments[0];
            if (arguments.Count > 1)
            {
                var lambda = GetLambda(arguments[1]);
                return new PostResultExpression(new FilterExpression(Visit(lambda.Body), Visit(source)), expressionType);
            }
            return new PostResultExpression(Visit(source), expressionType);
        }
        
        /// <summary>
        /// Determines whether a given expression can be executed locally. 
        /// (It contains no parts that should be translated to the target environment.)
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public bool CanBeEvaluatedLocally(Expression expression)
        {
            // any operation on a query can't be done locally
            var cex = expression as ConstantExpression;
            if (cex != null)
            {
                var query = cex.Value as IQueryable;
                if (query != null && query.Provider == this)
                    return false;
            }
            var mc = expression as MethodCallExpression;
            if (mc != null &&
                (mc.Method.DeclaringType == typeof(Enumerable) ||
                 mc.Method.DeclaringType == typeof(System.Linq.Queryable) ||
                 mc.Method.DeclaringType == typeof(Updatable))
                 )
            {
                return false;
            }
            if (expression.NodeType == ExpressionType.Convert &&
                expression.Type == typeof (object))
            {
                return true;
            }
            return expression.NodeType != ExpressionType.Parameter &&
                   expression.NodeType != ExpressionType.Lambda;
        }

        private static LambdaExpression GetLambda(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
            {
                e = ((UnaryExpression)e).Operand;
            }
            if (e.NodeType == ExpressionType.Constant)
            {
                return ((ConstantExpression)e).Value as LambdaExpression;
            }
            return e as LambdaExpression;
        }
    }
}