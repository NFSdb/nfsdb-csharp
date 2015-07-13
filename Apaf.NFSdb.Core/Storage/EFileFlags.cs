using System;

namespace Apaf.NFSdb.Core.Storage
{
    [Flags]
    public enum EFileFlags
    {
        None = 0,
        Sparse = 0x1
    }
}