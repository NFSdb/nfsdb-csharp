namespace Apaf.NFSdb.Core.Queries
{
    public static class RowIDUtil
    {
        public static int ToPartitionIndex(long rowID)
        {
            return (int)(rowID >> 44);
        }

        public static long ToLocalRowID(long rowID)
        {
            return rowID & 0xFFFFFFFFFFFL;
        }

        public static long ToRowID(int partitionIndex, long localRowID)
        {
            return (((long)partitionIndex) << 44) + localRowID;
        }  
    }
}