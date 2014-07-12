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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Apaf.NFSdb.Core.Tx;
using IQToolkit;
using IQToolkit.Data.Common;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class QueryPlanBinder<T> : DbExpressionVisitor
    {
        private readonly IJournal<T> _journal;
        private readonly IReadTransactionContext _tx;

        public QueryPlanBinder(IJournal<T> journal, IReadTransactionContext tx)
        {
            _journal = journal;
            _tx = tx;
        }

        public ResultSetBuilder<T> Build(Expression ex)
        {
            var translatedTree = Visit(ex);
            var evalVis = new ExpressionEvaluatorVisitor<T>(_journal, _tx);
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
            if (m.Method.DeclaringType == typeof(System.Linq.Queryable) 
                || m.Method.DeclaringType == typeof(Enumerable)
                || m.Method.DeclaringType == typeof(List<string>))
            {
                switch (m.Method.Name)
                {
                    case "Where":
                        return BindWhere(m.Type, m.Arguments[0], GetLambda(m.Arguments[1]));

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
                }
            }

            return base.VisitMethodCall(m);
        }
        private Expression BindContains(Expression source, Expression match)
        {
            var constSource = source as ConstantExpression;
            if (constSource != null && !IsQuery(constSource))
            {
                var values = new List<string>();
                var stringEnum = constSource.Value as IEnumerable<string>;
                if (stringEnum != null)
                {
                    values.AddRange(stringEnum);
                }
                else
                {
                    foreach (object value in (IEnumerable) constSource.Value)
                    {
                        var str = value as string;
                        if (str == null)
                        {
                            throw new NFSdbQuaryableNotSupportedException(
                                "List<string>.Contains, string[].Contains allowed only. Unable to execute" +
                                " Contains on source of type {0}", value);
                        }
                        values.Add((string) value);
                    }
                }
                match = Visit(match);
                return new SymbolContainsExpression(match, values.ToArray());
            }
            else
            {
                throw new NFSdbQuaryableNotSupportedException(
                           "List.Contains, Array.Contains allowed only. Unable to execute Contains on {0}.",
                           constSource);
            }
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
                throw new NFSdbQuaryableNotSupportedException(
                    "Usage of expressions in select statment is not allowed."
                    + " Please convert to IEnumerable and perform select statement.");
            }
            return Visit(predicate.Body);
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
                expression.Type == typeof(object))
                return true;
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