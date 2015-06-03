using System;
using System.Collections.Generic;
using System.Threading;
using Apaf.NFSdb.Core.Exceptions;

namespace Apaf.NFSdb.Core.Concurrency
{
    public class SharedExclusiveLock
    {
        public int ReadRefs;
        public Queue<WaitItem> WaitQueue = new Queue<WaitItem>();
        public int QueueLenght;

        public bool AcquireRead(AutoResetEvent waiter, bool enqueue)
        {
            int currentRead; 
            do
            {
                currentRead = ReadRefs;
                if (currentRead < 0)
                {
                    lock (this)
                    {
                        currentRead = ReadRefs;
                        if (currentRead < 0)
                        {
                            if (enqueue)
                            {
                                Interlocked.Increment(ref QueueLenght);
                                WaitQueue.Enqueue(new WaitItem
                                {
                                    IsRead = true,
                                    WaitHandle = waiter
                                });
                            }
                            return false;
                        }
                    }
                }
            } while (Interlocked.CompareExchange(ref ReadRefs, currentRead + 1, currentRead) != currentRead);

            return true;
        }


        public void ReleaseRead()
        {
            int readerCount;
            do
            {
                readerCount = ReadRefs;
                if (readerCount < 0)
                {
                    throw new NFSdbInvalidStateException(
                        "Write lock is held and since read lock cannot be released. Read count value " + readerCount);
                }
            } while (Interlocked.CompareExchange(ref ReadRefs, readerCount - 1, readerCount) != readerCount);

            // Decremented.
            readerCount = readerCount - 1;

            // No readers.
            if (readerCount == 0)
            {
                DispatchQueue();
            }
        }

        private void DispatchQueue()
        {
            if (QueueLenght > 0)
            {
                lock (this)
                {
                    int readerCount = ReadRefs;
                    if (readerCount == 0)
                    {
                        while (WaitQueue.Count > 0)
                        {
                            WaitItem wait = WaitQueue.Peek();
                            bool acquired = wait.IsRead
                                ? AcquireRead(wait.WaitHandle, false)
                                : AcquireWrite(wait.WaitHandle, false);

                            if (acquired)
                            {
                                WaitQueue.Dequeue();
                                Interlocked.Decrement(ref QueueLenght);

                                try
                                {
                                    wait.WaitHandle.Set();
                                }
                                catch (SystemException)
                                {
                                    if (wait.IsRead)
                                    {
                                        Interlocked.Decrement(ref ReadRefs);
                                    }
                                    else
                                    {
                                        Interlocked.Increment(ref ReadRefs);
                                    }
                                    continue;
                                }

                                if (!wait.IsRead)
                                {
                                    // Writer lock set. Queue cannot be moved further.
                                    return;
                                }
                            }
                            else
                            {
                                // Something else got writer lock.
                                return;
                            }
                        }
                    }
                }
            }
        }

        public bool AcquireWrite(AutoResetEvent waiter, bool enqueue)
        {
            do
            {
                int currentRead = ReadRefs;
                if (currentRead != 0)
                {
                    lock (this)
                    {
                        currentRead = ReadRefs;
                        if (currentRead != 0)
                        {
                            if (enqueue)
                            {
                                Interlocked.Increment(ref QueueLenght);
                                WaitQueue.Enqueue(new WaitItem
                                {
                                    IsRead = false,
                                    WaitHandle = waiter
                                });
                            }
                            return false;
                        }
                    }
                }
            } while (Interlocked.CompareExchange(ref ReadRefs, -1, 0) != 0);

            return true;
        }

        public void ReleaseWrite()
        {
            int readerCount;
            do
            {
                readerCount = ReadRefs;
                if (readerCount != -1)
                {
                    throw new NFSdbInvalidStateException(
                        "Write lock is not held and since cannot be released. Read count value " + readerCount);
                }
            } while (Interlocked.CompareExchange(ref ReadRefs, 0, readerCount) != readerCount);

            DispatchQueue();
        }

        public struct WaitItem
        {
            public AutoResetEvent WaitHandle;
            public bool IsRead;
        } 
    }
}