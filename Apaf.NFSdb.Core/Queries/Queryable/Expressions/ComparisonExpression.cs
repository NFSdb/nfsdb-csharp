using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class ComparisonExpression: QlExpression
    {
        public ComparisonExpression(Expression left, ExpressionType operation, Expression right, QlToken token)
            : base(token)
        {
            Left = left;
            Operation = operation;
            Right = right;
        }

        public ExpressionType Operation { get; private set; }
        public Expression Right { get; private set; }
        public Expression Left { get; private set; }

        public override ExpressionType NodeType
        {
            get { return Operation; }
        }

        public override string ToString()
        {
            return string.Format("({0} {1} {2})", Left, Operation, Right);
        }
    }
}