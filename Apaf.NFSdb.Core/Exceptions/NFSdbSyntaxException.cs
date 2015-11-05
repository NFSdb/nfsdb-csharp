using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbSyntaxException: NFSdbLockException
    {
        public NFSdbSyntaxException()
        {
        }

        public NFSdbSyntaxException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public NFSdbSyntaxException(string message, Exception inException, params object[] args)
            : base(string.Format(message, args), inException)
        {
        }
    }
}