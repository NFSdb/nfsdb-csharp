using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public interface IPartitionFilter
    {
        IEnumerable<long> Filter(IEnumerable<PartitionRowIDRange> partitions, 
            IReadTransactionContext transaction);
    }
}