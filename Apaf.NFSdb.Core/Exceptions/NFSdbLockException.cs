using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbLockException: NFSdbBaseExcepton
    {
        public NFSdbLockException()
        {
        }

        public NFSdbLockException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public NFSdbLockException(string message, Exception inException, params object[] args)
            : base(string.Format(message, args), inException)
        {
        }
    }
}