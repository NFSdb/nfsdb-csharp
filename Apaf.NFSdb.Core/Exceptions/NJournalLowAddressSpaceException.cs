using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbLowAddressSpaceException : NFSdbBaseExcepton
    {
        public NFSdbLowAddressSpaceException()
        {
        }

        public NFSdbLowAddressSpaceException(string message, params object[] args)
            :base(message, args)
        {
        }

        public NFSdbLowAddressSpaceException(string message, Exception innerException, params object[] args)
            : base(message, innerException, args)
        {
        }
    }
}