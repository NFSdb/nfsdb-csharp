using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    [Serializable]
    public class NFSdbEmptyFileException : NFSdbBaseExcepton
    {
        public NFSdbEmptyFileException()
        {
        }

        public NFSdbEmptyFileException(string message, params object[] args)
            : base(message, args)
        {
        }

        public NFSdbEmptyFileException(string message, Exception ex, params object[] args)
            : base(message, ex, args)
        {
        }
    }
}