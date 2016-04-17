using System;
using System.Linq;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Tx
{
    public class PartitionTxData
    {
        public PartitionTxData(int columnCount, int partitionID, ReadContext readContext)
            : this(columnCount, partitionID, DateTime.MinValue, DateTime.MaxValue, readContext)
        {
        }

        public PartitionTxData(int columnCount, int partitionID)
            : this(columnCount, partitionID, DateTime.MinValue, DateTime.MaxValue, new ReadContext())
        {
        }

        public PartitionTxData(int columnCount, int partitionID, DateTime startDate, DateTime endDate, ReadContext readContext)
        {
            PartitionID = partitionID;
            StartDate = startDate;
            EndDate = endDate;
            AppendOffset = new long[columnCount];
            SymbolData = Enumerable.Range(0, columnCount)
                .Select(dd => new SymbolTxData()).ToArray();
            ReadCache = readContext;
        }

        public bool IsPartitionUpdated;
        public long NextRowID;
        public long LastTimestamp;
        public readonly long[] AppendOffset;
        public readonly SymbolTxData[] SymbolData;
        public bool IsAppended;
        public readonly int PartitionID;
        public readonly DateTime StartDate;
        public readonly DateTime EndDate;
        public readonly ReadContext ReadCache;
    }
}