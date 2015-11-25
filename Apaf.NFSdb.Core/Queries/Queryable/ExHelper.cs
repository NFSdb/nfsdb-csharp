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

            if (member is ColumnNameExpression)
            {
                return ((ColumnNameExpression)member).Name;
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

        public static object GetLiteralValue(Expression expression)
        {
            var member = expression.GetLeft().NodeType == ExpressionType.Constant || expression.GetLeft().NodeType == (ExpressionType) EJournalExpressionType.Literal
                ? expression.GetLeft()
                : expression.GetRight();

            
            if (member is LiteralExpression)
            {
                member = ((LiteralExpression) member).Constant;
            }

            if (!(member is ConstantExpression))
            {
                throw QueryExceptionExtensions.ExpressionNotSupported("Unable to evaluate <{0}> Expressions of type column = " +
                                                                 "'literal' are supported only.", expression);
            }

            var memEx = (ConstantExpression)member;
            return memEx.Value;
        }
    }
}