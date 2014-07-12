using System;
using System.Diagnostics;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.TestModel.Model;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.IntegrationTests.Reading
{
    [TestFixture]
    [Category("Integration")]
    public class QuotePartitionTests
    {
        private const int ITEMS_COUNT = (int)1e6;

        [Test]
        [Category("Integration")]
        public void Should_read_first_quote_records()
        {
            var partition = CreatePartition();
            var v = partition.Read(0, new ReadContext());
            Assert.That(v.Sym, Is.Not.Null);
        }

        [Test]
        [Category("Integration")]
        public void Shoul_read_chunks_correctly()
        {
            var partition1 = CreatePartition(250);
            var partition2 = CreatePartition((int)1E6);
            var readContext = new ReadContext();
            for (int i = 0; i < 3E4; i+= 233)
            {
                var v1 = partition1.Read(i, readContext);
                var v2 = partition2.Read(i, readContext);

                Assert.AreEqual(v1.ToString(), v2.ToString());
            }
        }

        [Test]
        [Category("Integration")]
        public void Measure_read_speed()
        {
            var partition = CreatePartition();
            var sw = new Stopwatch();
            sw.Start();
            var rc = new ReadContext();
            for (int i = 0; i < ITEMS_COUNT; i++)
            {
                var v = partition.Read(i % 300000, rc);
            }
            sw.Stop();
            partition.Dispose();
            Console.WriteLine(sw.Elapsed);
        }

        private Partition<Quote> CreatePartition(int? recordHint = null, EFileAccess access = EFileAccess.Read)
        {
            var partData = Utils.CreatePartition<Quote>(recordHint, access);
            return partData.Partition;
        }
    }
}
