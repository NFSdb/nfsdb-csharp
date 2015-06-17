using System;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Tx
{
    public interface IPartitionAccess : IDisposable
    {
        IPartitionCore GetPartition();
    }
}