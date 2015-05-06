namespace Apaf.NFSdb.Core.Tx
{
    public class PartitionTxRec
    {
        public long RowCount;
        public long[] VarLenFileAppendOffsets;
    }
}