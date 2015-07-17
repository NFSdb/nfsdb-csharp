using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public interface IIndexedColumnCore
    {
        IEnumerable<long> GetValues(int valueKey, IReadTransactionContext tx);
        long GetCount(int valueKey, IReadTransactionContext tx);  
    }
}