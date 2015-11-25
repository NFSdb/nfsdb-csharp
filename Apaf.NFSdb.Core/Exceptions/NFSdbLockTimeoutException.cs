using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    [Serializable]
    public class NFSdbLockTimeoutException : NFSdbLockException
    {
        internal NFSdbLockTimeoutException()
        {
        }

        internal NFSdbLockTimeoutException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        internal NFSdbLockTimeoutException(string message, Exception inException, params object[] args)
            : base(string.Format(message, args), inException)
        {
        }
    }
}