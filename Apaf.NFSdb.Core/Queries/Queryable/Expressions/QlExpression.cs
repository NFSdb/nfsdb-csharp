using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public abstract class QlExpression : Expression
    {
        public QlToken ParseToken { get; private set; }

        protected QlExpression(QlToken parseToken)
        {
            ParseToken = parseToken;
        }
    }
}