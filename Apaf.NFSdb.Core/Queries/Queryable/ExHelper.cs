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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Apaf.NFSdb.Core.Queries.Queryable.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public static class ExHelper
    {
        public static string GetMemberName(Expression expression, Type journalType)
        {
            ExpressionType leftType = expression.GetLeft().NodeType;
            var member = leftType == ExpressionType.MemberAccess || leftType == (ExpressionType) EJournalExpressionType.Column
                ? expression.GetLeft()
                : expression.GetRight();

            var ex = member as MemberExpression;
            if (ex != null)
            {
                var memEx = ex;
                return GetMemberName(memEx, journalType);
            }

            var match = member as ColumnNameExpression;
            if (match != null)
            {
                return match.Name;
            }
            throw QueryExceptionExtensions.ExpressionNotSupported("Cannot extract column name from expression ", expression);
        }

        public static string GetMemberName(SymbolContainsExpression expression, Type journalType)
        {
            var ex = expression.Match as MemberExpression;
            if (ex != null)
            {
                var memEx = ex;
                return GetMemberName(memEx, journalType);
            }

            var match = expression.Match as ColumnNameExpression;
            if (match != null)
            {
                return match.Name;
            }
            throw QueryExceptionExtensions.ExpressionNotSupported("Cannot extract column name from expression ", expression);
        }

        public static string GetMemberName(MemberExpression memEx, Type journalType)
        {
            if (memEx.Member.DeclaringType == null || !memEx.Member.DeclaringType.IsAssignableFrom(journalType))
            {
                throw QueryExceptionExtensions.NotSupported("Expressions of type \"column\" == value "
                                                                 + "where column is an NFSdb property "
                                                                 + "name are supported only");
            }

            return memEx.Member.Name;
        }

        public static object GetLiteralValue(Expression parentExpression, IList<QlParameter> parameters)
        {
            Expression left = parentExpression.GetLeft();
            var literal = left.NodeType == ExpressionType.Constant 
                || left.NodeType == (ExpressionType) EJournalExpressionType.Literal
                || left.NodeType == (ExpressionType)EJournalExpressionType.Parameter
                ? left
                : parentExpression.GetRight();

            
            return GetLiteralValue(literal, parameters, parentExpression);
        }

        public static object GetLiteralValue(Expression literalExpression, IEnumerable<QlParameter> parameters, Expression parentExpression)
        {
            var valExp = literalExpression as ValueListExpression;
            if (valExp != null)
            {
                return valExp.Values.Select(v => GetLiteralValue(v, parameters, parentExpression)).ToList();
            }

            if (literalExpression is LiteralExpression)
            {
                literalExpression = ((LiteralExpression) literalExpression).Constant;
            }
            else
            {
                var exp = literalExpression as ParameterNameExpression;
                if (exp != null)
                {
                    var paramExp = exp;
                    QlParameter p;
                    if (parameters == null ||
                        (p =
                            parameters.FirstOrDefault(
                                pp => string.Equals(pp.Name, paramExp.Name, StringComparison.OrdinalIgnoreCase))) == null)
                    {
                        throw QueryExceptionExtensions.ExpressionNotSupported(
                            "Unable to evaluate <{0}>. Parameter value not passed.", parentExpression);
                    }
                    return p.Value;
                }
            }

            if (!(literalExpression is ConstantExpression))
            {
                throw QueryExceptionExtensions.ExpressionNotSupported(
                    "Unable to evaluate <{0}>. Expressions of type column = " +
                    "'literal' are supported only.", parentExpression);
            }

            var memEx = (ConstantExpression) literalExpression;
            return memEx.Value;
        }
    }
}