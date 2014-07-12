using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public abstract class NFSdbBaseExcepton : Exception
    {

        public NFSdbBaseExcepton()
        {
        }

        public NFSdbBaseExcepton(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public NFSdbBaseExcepton(string message, Exception inException, params object[] args)
            : base(string.Format(message, args), inException)
        {
        }
    }
}