using System;
using System.Linq.Expressions;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Queries.Queryable.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public static class QueryExceptionExtensions
    {
        public static NFSdbBaseExcepton NotSupported(string message, params object[] parameters)
        {
            return NFSdbQueryableNotSupportedException.Create(string.Format(message, parameters));
        }

        public static Exception ExpressionNotSupported(string message, Expression expression)
        {
            var qle = expression as QlExpression;
            if (qle != null)
            {
                if (qle.ParseToken != QlToken.QUERIABLE_TOKEN)
                {
                    if (message.Contains("{0}"))
                    {
                        message = string.Format(message, qle.ParseToken.Text);
                    }

                    message = string.Format("line {0}:{1} {2}",
                        qle.ParseToken.Line, qle.ParseToken.Position, message);
                }
                return NFSdbQueryableNotSupportedException.Create(message, qle.ParseToken);
            }

            if (message.Contains("{0}"))
            {
                return NFSdbQueryableNotSupportedException.Create(string.Format(message, expression));
            }
            return NFSdbQueryableNotSupportedException.Create(string.Format("{0} {1}", message, expression));
        }
    }
}