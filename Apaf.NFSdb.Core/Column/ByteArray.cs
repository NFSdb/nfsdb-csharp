namespace Apaf.NFSdb.Core.Column
{
    public struct ByteArray
    {
        private readonly byte[] _data;

        public ByteArray(byte[] data)
        {
            _data = data;
        }

        public bool IsSet(int index)
        {
            return (_data[index / 8] & (byte)(1 << (index % 8))) != 0;
        }

        public void Set(int index, bool value)
        {
            if (value)
            {
                _data[index / 8] |= (byte)(1 << (index % 8));
            }
            else
            {
                _data[index / 8] &= (byte)~(1 << (index % 8));
            }
        }
    }
}