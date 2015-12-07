using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class SliceExpression : PostResultExpression
    {
        public Expression Count { get; private set; }

        public SliceExpression(Expression body, EJournalExpressionType operation, Expression count) 
            : base(body, operation)
        {
            Count = count;
        }

        public SliceExpression(Expression body, EJournalExpressionType operation, Expression count, QlToken token)
            : base(body, operation, token)
        {
            Count = count;
        }
    }
}