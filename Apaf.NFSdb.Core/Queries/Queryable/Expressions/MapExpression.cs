using System.Collections.Generic;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class MapExpression : PostResultExpression
    {
        public List<ColumnNameExpression> Columns { get; private set; }

        public MapExpression(QlExpression result, List<ColumnNameExpression> columns, QlToken qlToken)
            : base(result, EJournalExpressionType.Map, qlToken)
        {
            Columns = columns;
        }

        public override string ToString()
        {
            return string.Format("{0} {1}", string.Join(", ", Columns), Body);
        }
    }
}