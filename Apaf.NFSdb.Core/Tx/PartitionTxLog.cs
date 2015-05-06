using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Tx
{
    public class PartitionTxLog
    {
        private const int LONG_ASCII_LEN = 20;
        private const byte ASCII_ZERO = (byte) '0';
        private const byte ASCII_NINE = (byte) '9';
        private const int HEADER_OFFSET = 64;
        private const int SEPARATOR_OFFSET = 64;

        public void ReadTxRecord(IRawFile txFile, PartitionTxRec record)
        {
            long offset = HEADER_OFFSET;
            record.RowCount = ReadLong(txFile, ref offset);
            offset += SEPARATOR_OFFSET;

            for (int i = 0; i < record.VarLenFileAppendOffsets.Length; i++)
            {
                record.VarLenFileAppendOffsets[i] = ReadLong(txFile, ref offset);
            }
        }

        public void WriteTxRecord(IRawFile txFile, PartitionTxRec record)
        {
            long offset = HEADER_OFFSET;
            WriteLong(txFile, record.RowCount, ref offset);
            offset += SEPARATOR_OFFSET;

            for (int i = 0; i < record.VarLenFileAppendOffsets.Length; i++)
            {
                WriteLong(txFile, record.VarLenFileAppendOffsets[i], ref offset);
            }
        }

        private void WriteLong(IRawFile txFile, long value, ref long offset)
        {
            for (int i = LONG_ASCII_LEN - 1; i >= 0; i--)
            {
                var write = (byte) (ASCII_ZERO + value%10);
                txFile.WriteByte(offset + i, write);
                value /= 10;
            }
        }

        private static long ReadLong(IRawFile txFile, ref long offset)
        {
            long result = 0;
            for (int i = 0; i < LONG_ASCII_LEN; i++)
            {
                var b = txFile.ReadByte(i + offset);
                if (b != 0 && b >= ASCII_ZERO && b <= ASCII_NINE)
                {
                    result *= 10;
                    result += b - ASCII_ZERO;
                }
                else
                {
                    break;
                }
            }
            offset += LONG_ASCII_LEN;
            return result;
        }
    }
}