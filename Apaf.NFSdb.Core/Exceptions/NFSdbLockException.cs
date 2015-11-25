using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    [Serializable]
    public class NFSdbLockException : NFSdbBaseExcepton
    {
        internal NFSdbLockException()
        {
        }

        internal NFSdbLockException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        internal NFSdbLockException(string message, Exception inException, params object[] args)
            : base(string.Format(message, args), inException)
        {
        }
    }
}