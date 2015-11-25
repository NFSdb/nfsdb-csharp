using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class QlToken
    {
        public static readonly QlToken QUERIABLE_TOKEN = new QlToken(MetadataConstants.IQUERYABLE_TOKEN_EXPRESSION, 0, 0);

        public QlToken(string text, int line, int position)
        {
            Position = position;
            Line = line;
            Text = text;
        }

        public string Text { get; private set; }
        public int Line { get; private set; }
        public int Position { get; private set; }
    }
}