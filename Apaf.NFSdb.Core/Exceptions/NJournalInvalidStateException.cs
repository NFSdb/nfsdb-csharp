using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbInvalidStateException: NFSdbBaseExcepton
    {
        public NFSdbInvalidStateException()
        {
        }

        public NFSdbInvalidStateException(string message, params object[] args)
            : base(message, args)
        {
        }

        public NFSdbInvalidStateException(string message, Exception ex, params object[] args)
            : base(message, ex, args)
        {
        }
    }
}