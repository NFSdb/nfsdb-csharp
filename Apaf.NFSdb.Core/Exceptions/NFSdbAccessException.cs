using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbAccessException: NFSdbBaseExcepton
    {
        internal NFSdbAccessException()
        {
        }

        internal NFSdbAccessException(string message, params object[] args)
            :base(message, args)
        {
        }

        internal NFSdbAccessException(string message, Exception innerException, params object[] args)
            : base(message, innerException, args)
        {
        }
    }
}