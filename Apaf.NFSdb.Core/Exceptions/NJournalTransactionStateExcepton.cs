using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbTransactionStateExcepton : NFSdbBaseExcepton
    {
        public NFSdbTransactionStateExcepton()
        {
        }

        public NFSdbTransactionStateExcepton(string message, params object[] args)
            : base(message, args)
        {
        }

        public NFSdbTransactionStateExcepton(string message, Exception innerException, params object[] args)
            : base(message, innerException, args)
        {
        }
    }
}