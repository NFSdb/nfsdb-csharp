using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbInvalidReadException : NFSdbBaseExcepton
    {
        public NFSdbInvalidReadException()
        {
        }

        public NFSdbInvalidReadException(string message, params object[] args)
            : base(message, args)
        {
        }

        public NFSdbInvalidReadException(string message, Exception ex, params object[] args)
            : base(message, ex, args)
        {
        }
    }
}