using System;
using Apaf.NFSdb.Core.Exceptions;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class NFSdbQuaryableNotSupportedException : NFSdbBaseExcepton
    {
        public NFSdbQuaryableNotSupportedException()
        {
        }

        public NFSdbQuaryableNotSupportedException(string message, params object[] args)
            : base(message, args)
        {
        }

        public NFSdbQuaryableNotSupportedException(string message, Exception ex, params object[] args)
            : base(message, ex, args)
        {
        }
    }
}