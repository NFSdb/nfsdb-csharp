using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class LiteralExpression : QlExpression
    {
        public LiteralExpression(ConstantExpression constant, QlToken token) : base(token)
        {
            Constant = constant;
        }

        public ConstantExpression Constant { get; private set; }

        public EJournalExpressionType Operation
        {
            get { return EJournalExpressionType.Literal; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)Operation; }
        }

        public override string ToString()
        {
            return Constant == null ? string.Empty : Constant.ToString();
        }
    }
}