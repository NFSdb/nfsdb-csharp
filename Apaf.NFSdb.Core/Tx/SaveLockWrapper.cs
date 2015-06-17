using System;
using System.Collections.Generic;
using System.Threading;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Tx
{
    public class SaveLockWrapper : ITxPartitionLock
    {
        private readonly TimeSpan _maxWait;
        private readonly IPartitionRangeLock _partitionManager;
        private int _partitionCount;
        private List<bool> _readLocked = new List<bool>();
        private List<bool> _writeLocked = new List<bool>();
        private bool _inUse;
        private readonly AutoResetEvent _waiter = new AutoResetEvent(false);

        public SaveLockWrapper(IPartitionRangeLock partitionManager) :this(TimeSpan.FromHours(1))
        {
            _partitionManager = partitionManager;
            _maxWait = TimeSpan.FromHours(1);
        }

        public SaveLockWrapper(TimeSpan maxWait)
        {
            _maxWait = maxWait;
        }

        public void Initialize(int maxPartitionID)
        {
            
            if (_inUse)
            {
                throw new InvalidOperationException("PartitionLockManager is already in use");
            }
            _partitionCount = maxPartitionID + 1;
            if (_readLocked == null)
            {
                _readLocked = new List<bool>();
                _writeLocked = new List<bool>();
            }
            else
            {
                _readLocked.Capacity = _partitionCount;
                _writeLocked.Capacity = _partitionCount;
            }
            int initialSize = _readLocked.Count;

            for (int i = 0; i < _partitionCount; i++)
            {
                if (i < initialSize)
                {
                    _readLocked[i] = false;
                    _writeLocked[i] = false;
                }
                else
                {
                    _readLocked.Add(false);
                    _writeLocked.Add(false);
                }
            }

            _inUse = true;
        }

        public void ReleaseReadLock(int paritionID)
        {
            CheckUsed();
            if (_readLocked[paritionID])
            {
                _partitionManager.ReleaseReadPartitionLock(paritionID);
                _readLocked[paritionID] = false;
            }
        }


        public void AcquireReadLock(int partitionID)
        {
            CheckUsed();
            if (!_readLocked[partitionID])
            {
                if (_writeLocked[partitionID])
                {
                    ReleaseWriteLock(partitionID);
                }

                if (!_partitionManager.AcquireReadPartitionLock(_waiter, partitionID))
                {
                    if (!_waiter.WaitOne(_maxWait))
                    {
                        throw new NFSdbLockTimeoutException("Max wait interval {0} expired on " +
                                                            "waiting to acquire read long on" +
                                                            " partition {1}",
                            _maxWait, partitionID);
                    }
                }
                _readLocked[partitionID] = true;
            }
        }

        public void ReleaseWriteLock(int paritionID)
        {
            CheckUsed();
            if (_writeLocked[paritionID])
            {
                _partitionManager.ReleaseWritePartitionLock(paritionID);
                _writeLocked[paritionID] = false;
            }
        }

        public void AcquireWriteLock(int partitionID)
        {
            CheckUsed();
            if (!_writeLocked[partitionID])
            {
                if (_readLocked[partitionID])
                {
                    ReleaseReadLock(partitionID);
                }

                if (!_partitionManager.AcquireReadPartitionLock(_waiter, partitionID))
                {
                    if (!_waiter.WaitOne(_maxWait))
                    {
                        throw new NFSdbLockTimeoutException("Max wait interval {0} expired on " +
                                                            "waiting to acquire read long on" +
                                                            " partition {1}",
                            _maxWait, partitionID);
                    }
                }
                _writeLocked[partitionID] = true;
            }
        }

        public void Free()
        {
            if (_inUse)
            {
                ReleaseAllLocks();
                _inUse = false;
            }
        }

        private void ReleaseAllLocks()
        {
            for (int i = 0; i < _readLocked.Count; i++)
            {
                if (_readLocked[i])
                {
                    ReleaseReadLock(i);
                    _readLocked[i] = false;
                }

                if (_writeLocked[i])
                {
                    ReleaseWriteLock(i);
                    _writeLocked[i] = false;
                }
            }
        }

        private void CheckUsed()
        {
            if (!_inUse)
            {
                throw new NFSdbInvalidStateException("PartitionLockManager cannot " +
                                                     "complete operation when in not used state");
            }
        }

        public void Dispose()
        {
            Free();
            _waiter.Dispose();
        }
    }
}