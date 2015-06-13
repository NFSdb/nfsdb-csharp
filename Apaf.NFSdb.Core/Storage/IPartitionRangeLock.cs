using System.Threading;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IPartitionRangeLock
    {
        bool AcquireReadPartitionLock(AutoResetEvent waiter, int partitionID);
        void ReleaseReadPartitionLock(int paritionID);

        bool AcquireWritePartitionLock(AutoResetEvent waiter, int partitionID);
        void ReleaseWritePartitionLock(int partitionID);

        void SizeToPartitionID(int size);
    }
}