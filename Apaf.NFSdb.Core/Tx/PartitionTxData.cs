using System.Collections.Generic;
using System.Linq;

namespace Apaf.NFSdb.Core.Tx
{
    public class PartitionTxData : IPartitionTxData
    {
        private PartitionTxData()
        {
        }

        public PartitionTxData(int columnCount)
        {
            AppendOffset = new long[columnCount];
            SymbolData = Enumerable.Range(0, columnCount)
                .Select(dd => new SymbolTxData()).ToArray();
        }

        public bool IsPartitionUpdated { get; set; }
        public long NextRowID { get; set; }
        public long LastTimestamp { get; set; }
        public long[] AppendOffset { get; set; }
        public IList<SymbolTxData> SymbolData { get; set; }
        public bool IsAppended { get; set; }

        public PartitionTxData DeepClone()
        {
            var c = new PartitionTxData
            {
                IsPartitionUpdated = IsPartitionUpdated,
                NextRowID = NextRowID,
                LastTimestamp = LastTimestamp,
                AppendOffset = AppendOffset.ToList().ToArray(),
                SymbolData = SymbolData.Select(
                    sd => sd.DeepClone()).ToArray(),
            };
            return c;
        }

        public IPartitionTxData SymbolPartition { get; set; }
    }
}