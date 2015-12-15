using System;

namespace Apaf.NFSdb.Core.Writes
{
    public interface IWriterState
    {
        DateTime GetTimestampDelegate(object o);
    }
}