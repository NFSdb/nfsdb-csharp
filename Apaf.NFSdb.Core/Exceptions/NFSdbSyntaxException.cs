using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbSyntaxException: NFSdbBaseExcepton
    {
        private readonly int _line = -1;
        private readonly int _pos= -1;

        public NFSdbSyntaxException()
        {
        }

        public NFSdbSyntaxException(string message)
            : base(message)
        {
        }

        public NFSdbSyntaxException(string message, int line, int pos)
            : base(string.Format("line {0}:{1} {2}", line, pos, message))
        {
            _line = line;
            _pos = pos;
        }

        public NFSdbSyntaxException(string message, int line, int position, Exception inException)
            : base(string.Format("line {0}:{1} {2}", line, position, message), inException)
        {
            _line = line;
            _pos = position;
        }

        public int Line { get { return _line; } }
        public int Position { get { return _pos; } }
    }
}