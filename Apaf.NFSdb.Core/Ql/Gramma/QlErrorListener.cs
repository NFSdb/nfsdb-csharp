using Antlr4.Runtime;
using Apaf.NFSdb.Core.Exceptions;

namespace Apaf.NFSdb.Core.Ql.Gramma
{
    public class QlErrorListener : BaseErrorListener, IAntlrErrorListener<int>
    {
        public override void SyntaxError(IRecognizer recognizer, IToken offendingSymbol, int line, int charPositionInLine, string msg,
            RecognitionException e)
        {
            throw new NFSdbSyntaxException(msg, line, charPositionInLine, e);
        }

        public void SyntaxError(IRecognizer recognizer, int offendingSymbol, 
            int line, int charPositionInLine, string msg,
            RecognitionException e)
        {
            throw new NFSdbSyntaxException(msg, line, charPositionInLine, e);
        }
    }
}