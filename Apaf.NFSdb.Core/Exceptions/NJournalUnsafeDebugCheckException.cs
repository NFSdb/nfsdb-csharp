using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbUnsafeDebugCheckException : NFSdbBaseExcepton
    {
        public NFSdbUnsafeDebugCheckException()
        {
        }

        public NFSdbUnsafeDebugCheckException(string message, params object[] args)
            : base(message, args)
        {
        }

        public NFSdbUnsafeDebugCheckException(string message, Exception innException, params object[] args)
            : base(message, args)
        {
        }
    }

}