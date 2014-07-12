using System;
using Apaf.NFSdb.Core.Exceptions;

namespace Apaf.NFSdb.Core.Tx
{
    public class NFSdbInvalidTxAddressException : NFSdbBaseExcepton
    {
        public NFSdbInvalidTxAddressException(long address)
        {
            Address = address;
        }

        public NFSdbInvalidTxAddressException(long address, string message, params object[] args)
            : base(message, args)
        {
            Address = address;
        }

        public NFSdbInvalidTxAddressException(long address, string message, Exception ex, params object[] args)
            : base(message, ex, args)
        {
            Address = address;
        }

        public long Address { get; private set; }
    }
}