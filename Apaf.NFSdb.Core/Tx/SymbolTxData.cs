namespace Apaf.NFSdb.Core.Tx
{
    public class SymbolTxData
    {
        public SymbolTxData()
        {
        }

        public SymbolTxData(bool isKeyCreated, int blockSize, long blockOffset)
        {
            KeyBlockCreated = isKeyCreated;
            KeyBlockSize = blockSize;
            KeyBlockOffset = blockOffset;
        }

        public bool KeyBlockCreated;
        public int KeyBlockSize;
        public long KeyBlockOffset;

        public SymbolTxData DeepClone()
        {
            return new SymbolTxData(false, KeyBlockSize, KeyBlockOffset);
        }
    }
}