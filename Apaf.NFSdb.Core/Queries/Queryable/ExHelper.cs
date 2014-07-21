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

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public static class ExHelper
    {
        public static string GetMemberName(BinaryExpression expression, Type journalType)
        {
            var member = expression.Left.NodeType == ExpressionType.MemberAccess
                ? expression.Left
                : expression.Right;

            if (!(member is MemberExpression))
            {
                throw new NFSdbQuaryableNotSupportedException("Expressions of type \"column\" == " +
                                                                 "value are supported only");
            }

            var memEx = (MemberExpression)member;
            return GetMemberName(memEx, journalType);
        }

        public static string GetMemberName(MemberExpression memEx, Type journalType)
        {
            if (memEx.Member.DeclaringType != journalType)
            {
                throw new NFSdbQuaryableNotSupportedException("Expressions of type \"column\" == value "
                                                                 + "where column is an NFSdb property "
                                                                 + "name are supported only");
            }

            return memEx.Member.Name;
        }

        public static object LiteralName(BinaryExpression expression, Type type)
        {
            var member = expression.Left.NodeType == ExpressionType.Constant
                ? expression.Left
                : expression.Right;

            if (!(member is ConstantExpression))
            {
                throw new NFSdbQuaryableNotSupportedException("Expressions of type \"column\" == " +
                                                                 "value are supported only");
            }

            var memEx = (ConstantExpression)member;
            return memEx.Value;
        }
    }
}