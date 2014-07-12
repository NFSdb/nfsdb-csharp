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