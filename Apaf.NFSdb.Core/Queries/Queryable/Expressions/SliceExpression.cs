using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class SliceExpression : PostResultExpression
    {
        public int Count { get; private set; }

        public SliceExpression(Expression body, EJournalExpressionType operation, int count) : base(body, operation)
        {
            Count = count;
        }
    }
}