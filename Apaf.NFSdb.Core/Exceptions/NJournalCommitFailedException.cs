using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbCommitFailedException : NFSdbBaseExcepton
    {
        public NFSdbCommitFailedException()
        {
        }

        public NFSdbCommitFailedException(string message, params object[] args)
            :base(message, args)
        {
        }

        public NFSdbCommitFailedException(string message, Exception innerException, params object[] args)
            : base(message, innerException, args)
        {
        }
    }
}