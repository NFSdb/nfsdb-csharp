namespace Apaf.NFSdb.Core.Storage
{
    public class ReadContext : IReadContext
    {
        public byte[] AllocateByteArray(int size)
        {
            return new byte[size];
        }

        public byte[] AllocateByteArray2(int size)
        {
            return new byte[size];
        }

        public byte[] AllocateByteArray3(int size)
        {
            return new byte[size];
        }
    }
}