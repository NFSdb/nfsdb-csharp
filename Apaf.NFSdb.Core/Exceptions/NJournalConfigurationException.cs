using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFSdbConfigurationException : NFSdbBaseExcepton
    {
        public NFSdbConfigurationException()
        {
        }

        public NFSdbConfigurationException(string message, params object[] args)
            : base(message, args)
        {
        }

        public NFSdbConfigurationException(string message, Exception ex, params object[] args)
            : base(message, ex, args)
        {
        }
    }
}