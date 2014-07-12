using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbReleasedPointerException : NFSdbBaseExcepton
    {
        public NFSdbReleasedPointerException()
        {
        }

        public NFSdbReleasedPointerException(string message, params object[] args)
            : base(message, args)
        {
        }

        public NFSdbReleasedPointerException(string message, Exception innerException, params object[] args)
            : base(message, innerException, args)
        {
        }
    }
}