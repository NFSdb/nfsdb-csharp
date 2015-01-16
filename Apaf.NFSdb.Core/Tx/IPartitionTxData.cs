using System.Collections.Generic;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Tx
{
    public interface IPartitionTxData
    {
        bool IsPartitionUpdated { get; set; }
        long NextRowID { get; set; }
        long LastTimestamp { get; set; }
        long[] AppendOffset { get; set; }
        IList<SymbolTxData> SymbolData { get; set; }
        bool IsAppended { get; set; }
        PartitionTxData DeepClone();

        IPartitionTxData SymbolPartition { get; set; }
    }
}