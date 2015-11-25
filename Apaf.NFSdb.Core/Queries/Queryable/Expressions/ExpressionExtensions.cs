using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public static class ExpressionExtensions
    {
        public static Expression GetLeft(this Expression expression)
        {
            var biExp = expression as BinaryExpression;
            if (biExp != null)
            {
                return biExp.Left;
            }
            var compEx = expression as ComparisonExpression;
            if (compEx != null)
            {
                return compEx.Left;
            }

            throw QueryExceptionExtensions.ExpressionNotSupported("Expressions does not have left expression.", expression);
        }

        public static Expression GetRight(this Expression expression)
        {
            var biExp = expression as BinaryExpression;
            if (biExp != null)
            {
                return biExp.Right;
            }
            var compEx = expression as ComparisonExpression;
            if (compEx != null)
            {
                return compEx.Right;
            }

            throw QueryExceptionExtensions.ExpressionNotSupported("Expressions {0} does not have left expression.", expression);
        }
    }
}