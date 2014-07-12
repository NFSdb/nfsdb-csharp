namespace Apaf.NFSdb.Core.Storage
{
    public interface IReadContext 
    {
        byte[] AllocateByteArray(int size);
        byte[] AllocateByteArray2(int size);
        byte[] AllocateByteArray3(int size);
    }
}