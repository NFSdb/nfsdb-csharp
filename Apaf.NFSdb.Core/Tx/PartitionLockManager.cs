//using System;
//using System.Threading;
//using Apaf.NFSdb.Core.Storage;
//
//namespace Apaf.NFSdb.Core.Tx
//{
//    public class PartitionLockManager : IPartitionTxSupport
//    {
//        private IPartitionRangeLock _partitionManager;
//        private int _partitionCount;
//        private bool[] _locked;
//        private int[] _operationBuff;
//        private bool _inUse;
//        private bool _fullyLocked;
//        private bool _fullyUnlocked = true;
//        private readonly ManualResetEventSlim _waiter = new ManualResetEventSlim();
//
//        public void Initialize(IPartitionRangeLock partitionManager, int maxPartitionID)
//        {
//            if (_inUse)
//            {
//                throw new InvalidOperationException("PartitionLockManager is already in use");
//            }
//            _partitionManager = partitionManager;
//            _partitionCount = maxPartitionID + 1;
//
//            if (_locked == null || _locked.Length < _partitionCount)
//            {
//                _locked = new bool[_partitionCount + 1];
//            }
//            else
//            {
//                for (int i = 0; i < _partitionCount; i++)
//                {
//                    _locked[i] = false;
//                }
//            }
//
//            if (_operationBuff == null || _operationBuff.Length < _partitionCount)
//            {
//                _operationBuff = new int[_partitionCount + 1];
//            }
//
//            _inUse = true;
//        }
//
//        public void ReleaseParitionLocks()
//        {
//            if (_fullyUnlocked)
//            {
//                return;
//            }
//
//            if (_fullyLocked)
//            {
//
//                for (int i = 0; i < _partitionCount; i++)
//                {
//                    _locked[i] = false;
//                }
//                _partitionManager.ReleaseParitionLocks();
//
//                _fullyLocked = false;
//                _fullyUnlocked = true;
//
//                return;
//            }
//            
//            for (int i = 0; i < _partitionCount; i++)
//            {
//                if (_locked[i])
//                {
//                    _partitionManager.ReleaseParitionLock(i);
//                    _locked[i] = false;
//                }
//            }
//
//            _fullyLocked = false;
//            _fullyUnlocked = true;
//        }
//
//        public void ReleaseParitionLock(int paritionID)
//        {
//            if (!_locked[paritionID])
//            {
//                _partitionManager.ReleaseParitionLock(paritionID);
//                _locked[paritionID] = true;
//
//                _fullyLocked = false;
//                _fullyUnlocked = false;
//            }
//        }
//
//        public void AcquirePartitionLocks()
//        {
//            if (_fullyLocked) return;
//
//            if (_fullyUnlocked)
//            {
//                for (int i = 0; i < _partitionCount; i++)
//                {
//                    _locked[i] = true;
//                }
//
//                if (!_partitionManager.AcquireReadParitionLocks(_waiter))
//                {
//                    _waiter.Wait();
//                }
//                _fullyUnlocked = false;
//                _fullyLocked = true;
//
//                return;
//            }
//
//            int pos = 0;
//            for (int i = 0; i < _partitionCount; i++)
//            {
//                if (!_locked[i])
//                {
//                    _operationBuff[pos++] = i;
//                    _locked[i] = true;
//                }
//            }
//
//            if (pos > 0)
//            {
//                if (!_partitionManager.AcquireReadParitionLocks(_waiter, _operationBuff, pos))
//                {
//                    _waiter.Wait();
//                }
//            }
//
//            _fullyUnlocked = false;
//            _fullyLocked = true;
//        }
//
//        public void AcquirePartitionLocks(int[] paritionIDBuffer, int bufferLen)
//        {
//            if (_fullyLocked) return;
//
//            int pos = 0;
//            for (int i = 0; i < bufferLen; i++)
//            {
//                int partitionID = paritionIDBuffer[i];
//                if (!_locked[partitionID])
//                {
//                    _operationBuff[pos++] = partitionID;
//                    _locked[partitionID] = true;
//                }
//            }
//
//            if (pos > 0)
//            {
//                if (!_partitionManager.AcquireReadParitionLocks(_waiter, _operationBuff, pos))
//                {
//                    _waiter.Wait();
//                }
//            }
//            _fullyUnlocked = false;
//            _fullyLocked = false;
//        }
//
//        public void Free()
//        {
//            if (_inUse)
//            {
//                ReleaseParitionLocks();
//                _inUse = false;
//            }
//        }
//    }
//}