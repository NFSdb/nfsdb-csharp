using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class UnionExpression: Expression
    {
        private readonly Expression _left;
        private readonly Expression _right;

        public UnionExpression(Expression left, Expression right)
        {
            _left = left;
            _right = right;
        }

        public EJournalExpressionType Operation
        {
            get { return EJournalExpressionType.Union; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)EJournalExpressionType.Union; }
        }

        public Expression Left
        {
            get { return _left; }
        }

        public Expression Right
        {
            get { return _right; }
        }
    }
}