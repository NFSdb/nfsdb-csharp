using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class ValueListExpression: QlExpression
    {
        public ValueListExpression(IList<Expression> expressions, QlToken token)
            : base(token)
        {
            Values = expressions;
        }

        public IList<Expression> Values { get; private set; }

        public EJournalExpressionType Operation
        {
            get { return EJournalExpressionType.ValueList; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)Operation; }
        }

        public override string ToString()
        {
            return Values == null ? string.Empty : string.Join(", ", Values.Select(v => v.ToString()));
        }
    }
}