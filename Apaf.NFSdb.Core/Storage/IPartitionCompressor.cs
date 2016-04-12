using System.Collections.Generic;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IPartitionCompressor<T>
    {
        IEnumerable<T> UpdatePartition(IEnumerable<T> recordsToKeep);
    }
}