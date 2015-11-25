using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class FilterExpression : QlExpression
    {
        private readonly Expression _filter;
        private readonly Expression _source;

        public FilterExpression(Expression filter, Expression source)
            : base(QlToken.QUERIABLE_TOKEN)
        {
            _filter = filter;
            _source = source;
        }

        public FilterExpression(Expression filter, Expression source, QlToken token)
            : base(token)
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

        public override string ToString()
        {
            return string.Format("From {0} Where {1}", Source, Filter);
        }
    }
}