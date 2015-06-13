using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apaf.NFSdb.Core.Concurrency;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Concurrency
{
    [TestFixture]
    public class SharedExclusiveLockTests
    {
        private SharedExclusiveLock CreateLock()
        {
            return new SharedExclusiveLock();
        }

        [Test]
        public void ShouldAllowMultipleReads()
        {
            SharedExclusiveLock lk = CreateLock();
            lk.AcquireRead(new AutoResetEvent(false));

            bool secondRead = lk.AcquireRead(new AutoResetEvent(false));
            Assert.That(secondRead, Is.EqualTo(true));
        }

        [Test]
        public void ShouldEnqueueReadAfterWrite()
        {
            SharedExclusiveLock lk = CreateLock();
            lk.AcquireWrite(new AutoResetEvent(false));

            bool secondRead = lk.AcquireRead(new AutoResetEvent(false));
            Assert.That(secondRead, Is.EqualTo(false));
        }

        [Test]
        public void ShouldEnqueueWriteAfterRead()
        {
            SharedExclusiveLock lk = CreateLock();
            lk.AcquireRead(new AutoResetEvent(false));

            bool write = lk.AcquireWrite(new AutoResetEvent(false));
            Assert.That(write, Is.EqualTo(false));
        }

        [Test]
        public void ShouldResumeAllReadersWhenWriterIsReleased()
        {
            SharedExclusiveLock lk = CreateLock();

            // First write.
            lk.AcquireWrite(new AutoResetEvent(false));

            const int readers = 10;
            var readerWaters = new AutoResetEvent[readers];

            for (int i = 0; i < readers; i++)
            {
                readerWaters[i] = new AutoResetEvent(false);
                bool wait = lk.AcquireRead(readerWaters[i]);

                Assert.That(wait, Is.EqualTo(false));
            }

            // Release first writer.
            lk.ReleaseWrite();

            // Should be set.
            Stopwatch sw = Stopwatch.StartNew();
            WaitHandle.WaitAll(readerWaters.Cast<WaitHandle>().ToArray(),
                TimeSpan.FromSeconds(5));

            sw.Stop();

            Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(0.5)));
        }

        [Test]
        public void ShouldResumeWriteWhenNoReader()
        {
            SharedExclusiveLock lk = CreateLock();
            lk.AcquireRead(new AutoResetEvent(false));

            var writeEvent = new AutoResetEvent(false);
            lk.AcquireWrite(writeEvent);
            writeEvent.Reset();

            lk.ReleaseRead();

            // Should be set.
            Stopwatch sw = Stopwatch.StartNew();
            writeEvent.WaitOne(TimeSpan.FromSeconds(5));
            sw.Stop();

            Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(0.5)));
        }

        [Test]
        public void ShouldResumeWriteWhenReadersHitZero()
        {
            SharedExclusiveLock lk = CreateLock();

            // First read.
            lk.AcquireRead(new AutoResetEvent(false));

            // First write.
            var writeEvent = new AutoResetEvent(false);
            lk.AcquireWrite(writeEvent);

            // Get N readers.
            const int readers = 10;
            for (int i = 0; i < readers; i++)
            {
                lk.AcquireRead(new AutoResetEvent(false));
            }

            // Release N readers.
            for (int i = 0; i < readers; i++)
            {
                lk.ReleaseRead();
            }

            // Release last reader.
            writeEvent.Reset();
            lk.ReleaseRead();

            // Should be set.
            Stopwatch sw = Stopwatch.StartNew();
            writeEvent.WaitOne(TimeSpan.FromSeconds(5));
            sw.Stop();

            Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(0.5)));
        }

        [Test]
        public void ShouldResumeWriteWhenWriterIsReleased()
        {
            SharedExclusiveLock lk = CreateLock();

            // First write.
            lk.AcquireWrite(new AutoResetEvent(false));

            // Second write.
            var writeEvent = new AutoResetEvent(false);
            bool wait = lk.AcquireWrite(writeEvent);

            Assert.That(wait, Is.EqualTo(false));

            // Release first writer.
            writeEvent.Reset();
            lk.ReleaseWrite();

            // Should be set.
            Stopwatch sw = Stopwatch.StartNew();
            writeEvent.WaitOne(TimeSpan.FromSeconds(5));
            sw.Stop();

            Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(0.5)));
        }

        [Test]
        public void ShouldTumbleTryReadWrite()
        {
            SharedExclusiveLock lk = CreateLock();

            const int threads = 100;
            const int sumCount = (int) 10E6;
            long totalSum = 0;
            AutoResetEvent[] waters = Enumerable.Range(0, threads)
                .Select(i => new AutoResetEvent(false))
                .ToArray();

            Stopwatch sw = Stopwatch.StartNew();
            const long sumToMillion = (long) (sumCount - 1)*(sumCount)/2;

            Parallel.For(0, threads, i =>
            {
                bool needWait = i == 3 ? !lk.AcquireWrite(waters[i]) : !lk.AcquireRead(waters[i]);
                if (needWait)
                {
                    waters[i].WaitOne(TimeSpan.FromMinutes(2));
                }

                long sum = 0;

                // Make CPU busy.
                for (int j = 0; j < sumCount; j++)
                {
                    sum += j;
                }
                Interlocked.Add(ref totalSum, sum);

                if (i == 3)
                {
                    lk.ReleaseWrite();
                }
                else
                {
                    lk.ReleaseRead();
                }
            });

            sw.Stop();
            Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(10)));
            Assert.That(totalSum, Is.EqualTo(sumToMillion*threads));
        }
    }
}