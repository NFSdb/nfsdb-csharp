using System.Collections.Generic;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Tx
{
    public class TxState
    {
        public PartitionTxData[] PartitionDataStorage;
        public ReadContext ReadContext;
        public List<IPartition> Partitions;
        public List<bool> Locks;
    }
}