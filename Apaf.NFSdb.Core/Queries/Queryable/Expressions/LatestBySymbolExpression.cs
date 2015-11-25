using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class LatestBySymbolExpression : QlExpression
    {
        private readonly Expression _body;

        public LatestBySymbolExpression(string latestBy, Expression body)
            : base(QlToken.QUERIABLE_TOKEN)
        {
            LatestBy = latestBy;
            _body = body;
        }

        public LatestBySymbolExpression(string latestBy, Expression body, QlToken token)
            : base(token)
        {
            LatestBy = latestBy;
            _body = body;
        }

        public string LatestBy { get; private set; }

        public EJournalExpressionType Operation
        {
            get { return EJournalExpressionType.LatestBy; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType) Operation; }
        }

        public Expression Body
        {
            get { return _body; }
        }

        public override string ToString()
        {
            return string.Format("{0} Latest By {1}", Body, LatestBy);
        }
    }
}