using Antlr4.Runtime;
using Apaf.NFSdb.Core.Queries.Queryable;

namespace Apaf.NFSdb.Core.Ql.Gramma
{
    public static class TokenExtension 
    {
        public static QlToken ToQlToken(this ParserRuleContext context)
        {
            return new QlToken(context.GetText(), context.Start.Line, context.Start.StartIndex);
        }
    }
}