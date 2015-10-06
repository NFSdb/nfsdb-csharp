using System;
using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class LatestBySymbolExpression : Expression
    {
        private readonly Type _bodyType;
        private readonly Expression _body;

        public LatestBySymbolExpression(string latestBy, Type bodyType)
        {
            LatestBy = latestBy;
            _bodyType = bodyType;
        }

        public LatestBySymbolExpression(string latestBy, Expression body)
        {
            LatestBy = latestBy;
            _body = body;
            _bodyType = body.Type;
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

        public override Type Type
        {
            get { return _bodyType; }
        }

        public Expression Body
        {
            get { return _body; }
        }
    }
}