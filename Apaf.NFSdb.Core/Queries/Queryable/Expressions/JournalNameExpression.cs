using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class JournalNameExpression: Expression
    {
        public JournalNameExpression(string name)
        {
            Name = name;
        }

        public string Name { get; private set; }

        public EJournalExpressionType Operation
        {
            get { return EJournalExpressionType.Journal; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType) Operation; }
        }

        public override string ToString()
        {
            return Name;
        }
    }
}