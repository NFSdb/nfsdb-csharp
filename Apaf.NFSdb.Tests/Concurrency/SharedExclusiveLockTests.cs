using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Apaf.NFSdb.Core.Concurrency;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Concurrency
{
    [TestFixture]
    public class SharedExclusiveLockTests
    {
        [Test]
        public void ShouldAllowMultipleReads()
        {
            var lk = CreateLock();
            lk.AcquireRead(new AutoResetEvent(false), true);
            
            var secondRead = lk.AcquireRead(new AutoResetEvent(false), true);
            Assert.That(secondRead, Is.EqualTo(true));
        }

        [Test]
        public void ShouldEnqueueWriteAfterRead()
        {
            var lk = CreateLock();
            lk.AcquireRead(new AutoResetEvent(false), true);

            var write = lk.AcquireWrite(new AutoResetEvent(false), true);
            Assert.That(write, Is.EqualTo(false));
        }

        [Test]
        public void ShouldEnqueueReadAfterWrite()
        {
            var lk = CreateLock();
            lk.AcquireWrite(new AutoResetEvent(false), true);

            var secondRead = lk.AcquireRead(new AutoResetEvent(false), true);
            Assert.That(secondRead, Is.EqualTo(false));
        }

        [Test]
        public void ShouldResumeWriteWhenNoReader()
        {
            var lk = CreateLock();
            lk.AcquireRead(new AutoResetEvent(false), true);

            var writeEvent = new AutoResetEvent(false);
            lk.AcquireWrite(writeEvent, true);
            writeEvent.Reset();

            lk.ReleaseRead();

            // Should be set.
            var sw = Stopwatch.StartNew();
            writeEvent.WaitOne(TimeSpan.FromSeconds(5));
            sw.Stop();

            Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(0.5)));
        }


        [Test]
        public void ShouldResumeWriteWhenReadersHitZero()
        {
            var lk = CreateLock();

            // First read.
            lk.AcquireRead(new AutoResetEvent(false), true);

            // First write.
            var writeEvent = new AutoResetEvent(false);
            lk.AcquireWrite(writeEvent, true);

            // Get N readers.
            const int readers = 10;
            for (int i = 0; i < readers; i++)
            {
                lk.AcquireRead(new AutoResetEvent(false), true);
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
            var sw = Stopwatch.StartNew();
            writeEvent.WaitOne(TimeSpan.FromSeconds(5));
            sw.Stop();

            Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(0.5)));
        }


        [Test]
        public void ShouldResumeWriteWhenWriterIsReleased()
        {
            var lk = CreateLock();

            // First write.
            lk.AcquireWrite(new AutoResetEvent(false), true);

            // Second write.
            var writeEvent = new AutoResetEvent(false);
            var wait = lk.AcquireWrite(writeEvent, true);

            Assert.That(wait, Is.EqualTo(false));

            // Release first writer.
            writeEvent.Reset();
            lk.ReleaseWrite();

            // Should be set.
            var sw = Stopwatch.StartNew();
            writeEvent.WaitOne(TimeSpan.FromSeconds(5));
            sw.Stop();

            Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(0.5)));
        }

        [Test]
        public void ShouldResumeAllReadersWhenWriterIsReleased()
        {
            var lk = CreateLock();

            // First write.
            lk.AcquireWrite(new AutoResetEvent(false), true);

            const int readers = 10;
            var readerWaters = new AutoResetEvent[readers];

            for (int i = 0; i < readers; i++)
            {
                readerWaters[i] = new AutoResetEvent(false);
                var wait = lk.AcquireRead(readerWaters[i], true);

                Assert.That(wait, Is.EqualTo(false));
            }

            // Release first writer.
            lk.ReleaseWrite();

            // Should be set.
            var sw = Stopwatch.StartNew();
            WaitHandle.WaitAll(readerWaters.Cast<WaitHandle>().ToArray(),
                TimeSpan.FromSeconds(5));

            sw.Stop();

            Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(0.5)));
        }

        private SharedExclusiveLock CreateLock()
        {
            return new SharedExclusiveLock();
        }
    }
}