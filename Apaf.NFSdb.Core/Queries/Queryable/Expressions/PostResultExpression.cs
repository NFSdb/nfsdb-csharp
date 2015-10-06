using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class PostResultExpression: Expression
    {
        private readonly Expression _body;
        private readonly EJournalExpressionType _operation;

        public PostResultExpression(Expression body, EJournalExpressionType operation)
        {
            _body = body;
            _operation = operation;
        }

        public EJournalExpressionType Operation
        {
            get { return _operation; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)_operation; }
        }

        public Expression Body
        {
            get { return _body; }
        }
    }
}