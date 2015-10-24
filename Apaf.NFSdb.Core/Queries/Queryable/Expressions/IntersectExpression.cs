using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class FilterExpression : Expression
    {
        private readonly Expression _filter;
        private readonly Expression _source;

        public FilterExpression(Expression filter, Expression source)
        {
            _filter = filter;
            _source = source;
        }

        public EJournalExpressionType Operation
        {
            get { return EJournalExpressionType.Intersect; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)EJournalExpressionType.Filter; }
        }

        public Expression Filter
        {
            get { return _filter; }
        }

        public Expression Source
        {
            get { return _source; }
        }
    }
}