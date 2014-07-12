using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbStaleReadException : NFSdbBaseExcepton
    {
        public NFSdbStaleReadException()
        {
        }

        public NFSdbStaleReadException(string message, params object[] args)
            :base(message, args)
        {
        }

        public NFSdbStaleReadException(string message, Exception innerException, params object[] args)
            : base(message, innerException, args)
        {
        }
    }
}