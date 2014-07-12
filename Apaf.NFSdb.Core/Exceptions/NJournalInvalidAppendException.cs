using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbInvalidAppendException : NFSdbBaseExcepton
    {
        public NFSdbInvalidAppendException()
        {
        }

        public NFSdbInvalidAppendException(string message, params object[] args)
            :base(message, args)
        {
        }

        public NFSdbInvalidAppendException(string message, Exception innerException, params object[] args)
            : base(message, innerException, args)
        {
        }
    }
}