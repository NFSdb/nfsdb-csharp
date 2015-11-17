using System;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using Apaf.NFSdb.Core.Ql.Gramma;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Records
{
    public class RecordQuery : IRecordQuery
    {
        private readonly IJournalCore _journal;
        private readonly IReadTransactionContext _tx;

        public RecordQuery(IJournalCore journal, IReadTransactionContext tx)
        {
            _journal = journal;
            _tx = tx;
        }

        public void Dispose()
        {
            _tx.Dispose();
        }

        public IRecordSet Execute(string query)
        {
            var input = new AntlrInputStream(query);
            var lexer = new QlLexer(input);
            var commonTokenStream = new CommonTokenStream(lexer);
            var parser = new QlParser(commonTokenStream);
            parser.AddErrorListener(new QlErrorListener());
            parser.BuildParseTree = true;
            IParseTree tree = parser.parse();

            var lis = new QlVisitor();
            var expr = lis.Visit(tree);

            throw new NotImplementedException();
        }
    }
}