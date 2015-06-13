using System;
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

        public void SizeToPartitionID(int partitionID)
        {
            int newLength = partitionID + 1;
            if (newLength > _capacity)
            {
                _capacity = newLength + ADDITIONAL_CAPACITY;
                var newLocks = new SharedExclusiveLock[_capacity];
                Array.Copy(_locks, newLocks, _locks.Length);
                newLocks[newLength - 1] = new SharedExclusiveLock();

                Thread.MemoryBarrier();
                _locks = newLocks;
            }
            else
            {
                for (int i = _length; i < newLength; i++)
                {
                    _locks[i] = new SharedExclusiveLock();
                }
            }

            _length = newLength;
        }

        public bool AcquireReadPartitionLock(AutoResetEvent waiter, int partitionID)
        {
            return _locks[partitionID].AcquireRead(waiter);
        }

        public void ReleaseReadPartitionLock(int partitionID)
        {
            _locks[partitionID].ReleaseRead();
        }

        public bool AcquireWritePartitionLock(AutoResetEvent waiter, int partitionID)
        {
            return _locks[partitionID].AcquireWrite(waiter);
        }

        public void ReleaseWritePartitionLock(int partitionID)
        {
            _locks[partitionID].ReleaseWrite();
        }
    }
}