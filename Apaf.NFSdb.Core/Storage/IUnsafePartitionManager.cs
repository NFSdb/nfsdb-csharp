using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    internal interface IUnsafePartitionManager : IPartitionManagerCore
    {
        IColumnStorage SymbolFileStorage { get; }
        IPartitionCore GetPartition(int paritionID);
        IEnumerable<IPartitionCore> GetOpenPartitions();
        void Recycle(TxReusableState state);
    }
}