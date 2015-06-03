namespace Apaf.NFSdb.Core.Storage
{
    public interface IPartitionTxSupport
    {
        void ReleaseParitionLock(int paritionID);

        void AcquirePartitionLock(int partitionID);

        void Free();
    }
}