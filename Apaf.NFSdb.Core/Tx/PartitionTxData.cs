using System;
using System.Collections.Generic;
using System.Linq;

namespace Apaf.NFSdb.Core.Tx
{
    public class PartitionTxData
    {

        public PartitionTxData(int columnCount, int partitionID)
            : this(columnCount, partitionID, DateTime.MinValue, DateTime.MaxValue)
        {
        }

        public PartitionTxData(int columnCount, int partitionID, DateTime startDate, DateTime endDate)
        {
            PartitionID = partitionID;
            StartDate = startDate;
            EndDate = endDate;
            AppendOffset = new long[columnCount];
            SymbolData = Enumerable.Range(0, columnCount)
                .Select(dd => new SymbolTxData()).ToArray();
        }

        public bool IsPartitionUpdated;
        public long NextRowID;
        public long LastTimestamp;
        public readonly long[] AppendOffset;
        public readonly IList<SymbolTxData> SymbolData;
        public bool IsAppended;
        public readonly int PartitionID;
        public readonly DateTime StartDate;
        public readonly DateTime EndDate;
    }
}