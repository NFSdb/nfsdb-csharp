using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbPartitionException : NFSdbBaseExcepton
    {
        public NFSdbPartitionException()
        {
        }

        public NFSdbPartitionException(string message, params object[] args)
            : base(message, args)
        {
        }

        public NFSdbPartitionException(string message, Exception innException, params object[] args) :
            base(message, innException, args)
        {
        }
    }
}