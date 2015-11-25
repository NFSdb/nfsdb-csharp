using Antlr4.Runtime;
using Apaf.NFSdb.Core.Queries.Queryable;

namespace Apaf.NFSdb.Core.Ql.Gramma
{
    public static class TokenExtension 
    {
        public static QlToken ToQlToken(this IToken token)
        {
            return new QlToken(token.Text, token.Line, token.StartIndex);
        }
    }
}