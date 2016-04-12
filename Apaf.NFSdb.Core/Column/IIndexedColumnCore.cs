using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public interface IIndexedColumnCore
    {
        IEnumerable<long> GetValues(int valueKey, PartitionTxData tx);
        long GetCount(int valueKey, PartitionTxData tx);  
    }
}