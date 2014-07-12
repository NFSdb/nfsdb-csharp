namespace Apaf.NFSdb.Core.Queries
{
    public class PartitionRowIDRange
    {
        public PartitionRowIDRange(IPartitionCore part)
        {
            Partition = part;
            Low = 0;
            High = long.MaxValue;
        }

        public PartitionRowIDRange(IPartitionCore part, long low, long high)
        {
            Partition = part;
            Low = low;
            High = high;
        }

        public IPartitionCore Partition { get; private set; }
        public long Low { get; private set; }
        public long High { get; private set; }
    }
}