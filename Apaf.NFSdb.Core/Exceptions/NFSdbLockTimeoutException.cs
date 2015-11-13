using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    [Serializable]
    public class NFSdbLockTimeoutException : NFSdbLockException
    {
        public NFSdbLockTimeoutException()
        {
        }

        public NFSdbLockTimeoutException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public NFSdbLockTimeoutException(string message, Exception inException, params object[] args)
            : base(string.Format(message, args), inException)
        {
        }
    }
}