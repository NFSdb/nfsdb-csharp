using System;
using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class SymbolContainsExpression : Expression
    {
        private readonly Expression _match;
        private readonly string[] _values;

        public SymbolContainsExpression(Expression match, string[] values)
        {
            _match = match;
            _values = values;
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)JournalExpressionType.Contains; }
        }

        public override Type Type
        {
            get { return typeof(bool); }
        }

        public Expression Match
        {
            get { return _match; }
        }

        public string[] Values
        {
            get { return _values; }
        }
    }
}