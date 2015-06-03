using System.Threading;
using Apaf.NFSdb.Core.Concurrency;

namespace Apaf.NFSdb.Core.Storage
{
    public class PartitionRangeLock : IPartitionRangeLock
    {
        private const int ADDITIONAL_CAPACITY = 100;
        private int _capacity;
        private int _length;
        private SharedExclusiveLock[] _locks;

        public PartitionRangeLock(int partitionCount)
        {
            _capacity = partitionCount + ADDITIONAL_CAPACITY;
            _locks = new SharedExclusiveLock[_capacity];
            _length = partitionCount;
            for (int i = 0; i < _length; i++)
            {
                _locks[i] = new SharedExclusiveLock();
            }
        }

        public bool AcquireReadPartitionLock(AutoResetEvent waiter, int partitionID)
        {
            return _locks[partitionID].AcquireRead(waiter, true);
        }

        public void ReleaseReadPartitionLock(int partitionID)
        {
            _locks[partitionID].ReleaseRead();
        }

        public bool AcquireWritePartitionLock(AutoResetEvent waiter, int partitionID)
        {
            return _locks[partitionID].AcquireWrite(waiter, true);
        }

        public void ReleaseWritePartitionLock(int partitionID)
        {
            _locks[partitionID].ReleaseWrite();
        }
    }
}