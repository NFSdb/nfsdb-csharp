using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbInitializationException : NFSdbBaseExcepton
    {
        public NFSdbInitializationException()
        {
        }

        public NFSdbInitializationException(string message, params object[] args)
            : base(message, args)
        {
        }

        public NFSdbInitializationException(string message, Exception ex, params object[] args)
            : base(message, ex, args)
        {
        }
    }
}