using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public interface IPartitionIntervalIterator
    {
        IEnumerable<PartitionRowIDRange> IteratePartitions(IEnumerable<IPartitionCore> partitions,
            DateInterval interval, IReadTransactionContext transaction);
    }
}