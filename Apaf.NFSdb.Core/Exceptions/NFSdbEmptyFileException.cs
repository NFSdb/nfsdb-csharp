using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    [Serializable]
    public class NFSdbEmptyFileException : NFSdbBaseExcepton
    {
        internal NFSdbEmptyFileException()
        {
        }

        internal NFSdbEmptyFileException(string message, params object[] args)
            : base(message, args)
        {
        }

        internal NFSdbEmptyFileException(string message, Exception ex, params object[] args)
            : base(message, ex, args)
        {
        }
    }
}