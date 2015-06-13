using System;

namespace Apaf.NFSdb.Core.Storage
{
    public interface ITxPartitionLock : IDisposable
    {
        void ReleaseReadLock(int paritionID);

        void AcquireReadLock(int partitionID);

        void ReleaseWriteLock(int paritionID);

        void AcquireWriteLock(int partitionID);

        void Initialize(int maxPartitionID);

        void Free();
    }
}