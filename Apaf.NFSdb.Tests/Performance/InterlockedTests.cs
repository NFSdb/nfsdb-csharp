using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using Apaf.NFSdb.Core.Concurrency;
using NUnit.Framework;
using NUnit.Framework.Constraints;

namespace Apaf.NFSdb.Tests.Performance
{
    [TestFixture]
    [Explicit]
    public class InterlockedTests
    {
        [Test]
        // [Explicit]
        public void TestStraightSumSpeed()
        {
            int sum = 0;
            var sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < (int)100E6; i++)
            {
                sum++;
            }
            sw.Stop();
            Console.Write(sw.Elapsed);
        }


        [Test]
        // [Explicit]
        public void TestLockedSumSpeed()
        {
            int sum = 0;
            var lockObj = new object();
            var sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < (int)100E6; i++)
            {
                lock (lockObj)
                {
                    sum++;
                }
            }
            sw.Stop();
            Console.Write(sw.Elapsed);
        }

        [Test]
        // [Explicit]
        public void TestInterlockedSumSpeed()
        {
            int sum = 0;
            var sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < (int) 100E6; i++)
            {
                Interlocked.Increment(ref sum);
            }
            sw.Stop();
            Console.Write(sw.Elapsed);
        }

        [Test]
        // [Explicit]
        public void TestInterlockedCompareExchangeSumSpeed()
        {
            int sum = 0;
            var sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < (int) 100E6; i++)
            {
                Interlocked.CompareExchange(ref sum, sum + 1, sum);
            }
            sw.Stop();
            Console.Write(sw.Elapsed);
        }


        [Test]
        // [Explicit]
        public void TestReadWriteLockSlimSpeed()
        {
            int sum = 0;
            var rwl = new ReaderWriterLockSlim();
            var sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < (int)100E6; i++)
            {
                try
                {
                    rwl.EnterReadLock();
                    sum++;
                }
                finally
                {
                    rwl.ExitReadLock();
                }
            }
            sw.Stop();
            Console.Write(sw.Elapsed);
        }


        [Test]
        // [Explicit]
        public void TestSharedExclusiveLockSpeed()
        {
            int sum = 0;
            var rwl = new SharedExclusiveLock();
            var sw = new Stopwatch();
            var ev = new AutoResetEvent(false);

            sw.Start();
            for (int i = 0; i < (int)100E6; i++)
            {
                try
                {
                    rwl.AcquireRead(ev, true);
                    sum++;
                }
                finally
                {
                    rwl.ReleaseRead();
                }
            }
            sw.Stop();
            Console.Write(sw.Elapsed);
        }

        [Test]
        [Ignore]
        public unsafe void TestGCFixSpeed()
        {
            int sum = 0;
            var sw = new Stopwatch();
            sw.Start();

            object o = new FieldAccessException();
            TypedReference tr = __makeref(o);
            IntPtr ptr = **(IntPtr**)(&tr);

            for (int i = 0; i < 1E6; i++)
            {
                var pined = GCHandle.Alloc(o, GCHandleType.Pinned);
                var ptr2 = pined.AddrOfPinnedObject().ToPointer();
            }
            sw.Stop();
            Console.Write(sw.Elapsed);
        }
    }
}