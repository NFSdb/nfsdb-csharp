using System;
using System.Diagnostics;
using System.IO;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Writes;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.IntegrationTests.Reading
{
    [TestFixture]
    public class PocoQuoteJournalTests
    {
        private const int GENERATE_RECORDS_COUNT = TestUtils.GENERATE_RECORDS_COUNT;
        private const string FolderPath = "PocoQuote";

        private static readonly DateTime START = new DateTime(DateTime.Now.AddYears(-1).Year,
            DateTime.Now.AddYears(-1).Month, 1);
        private static readonly long START_TIMESTAMP = DateUtils.DateTimeToUnixTimeStamp(START);

        public static readonly string[] SYMBOLS = { "AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L" };

        public class PocoQuote
        {
            public long Timestamp { get; set; }
            public string Sym { get; set; }
            public double? Bid { get; set; }
            public double? Ask { get; set; }
            public int? BidSize { get; set; }
            public int AskSize { get; set; }
            public string Mode { get; set; }
            public string Ex { get; set; }
        }

        private IJournal<PocoQuote> CreateJournal(EFileAccess access = EFileAccess.ReadWrite)
        {
            Utils.ClearJournal<PocoQuote>(FolderPath);
            return OpenJournal(access);
        }

        private static IJournal<PocoQuote> OpenJournal(EFileAccess access)
        {
            string directoryPath = Path.Combine(Utils.FindJournalsPath(), FolderPath);

            return new JournalBuilder()
                .WithRecordCountHint(GENERATE_RECORDS_COUNT)
                .WithPartitionBy(EPartitionType.Month)
                .WithLocation(directoryPath)
                .WithSymbolColumn("Sym", 20, 5, 5)
                .WithSymbolColumn("Ex", 20, 20, 20)
                .WithSymbolColumn("Mode", 20, 20, 20)
                .WithTimestampColumn("Timestamp")
                .ToJournal<PocoQuote>(access);
        }

        public static TimeSpan GenerateRecords(IJournal<PocoQuote> journal, int count, int partitionCount)
        {
            var increment = GetTimestampIncrement(count, partitionCount);
            var stopwatch = new Stopwatch();
            using (var wr = journal.OpenWriteTx())
            {
                stopwatch.Start();
                var quote = new PocoQuote();
                for (int i = 0; i < count; i++)
                {
                    GenerateQuoteValues(quote, increment, i);
                    wr.Append(quote);
                }
                wr.Commit();
            }
            stopwatch.Stop();
            return stopwatch.Elapsed;
        }

        public static void GenerateQuoteValues(PocoQuote trade, long incrementMs, long i)
        {
            trade.Timestamp = START_TIMESTAMP + incrementMs * i;
            trade.Bid = i * 2.04;
            trade.Bid = i;
            trade.BidSize = (int)i;
            trade.Ask = i * 50.09014;
            trade.AskSize = (int)(i % int.MaxValue);
            trade.Ex = "LXE";
            trade.Mode = "Fast trading";
            trade.Sym = SYMBOLS[i % SYMBOLS.Length];
        }

        public static long GetTimestampIncrement(long count, int partitionCount)
        {
            return (long)((START.AddMonths(partitionCount).AddDays(-1) - START).TotalMilliseconds / count);
        }

        [Test]
        [Category("Performance")]
        public void Should_read_all_rows()
        {
            var totalCount = (int)(5 * GENERATE_RECORDS_COUNT);
            using (var journal = CreateJournal())
            {
                var elapsed = GenerateRecords(journal, totalCount, 2);
                Console.WriteLine(elapsed);
            }

            using (var j = OpenJournal(EFileAccess.Read))
            {
                var q = j.OpenReadTx();
                long count = 0;
                var sw = new Stopwatch();
                sw.Start();

                foreach (PocoQuote quote in q.All())
                {
                    var num = quote;
                    count++;
                }

                sw.Stop();
                Console.WriteLine(sw.Elapsed);
                Assert.That(count, Is.EqualTo(totalCount));
            }
        }
        
        [Test]
        public void Read_records_match_written_records()
        {
            const int totalCount = 10000;
            const int partitionCount = 3;
            using (var journal = CreateJournal())
            {
                GenerateRecords(journal, totalCount, partitionCount);
            }

            using (var j = OpenJournal(EFileAccess.Read))
            {
                var q = j.OpenReadTx();
                long count = 0;
                var expected = new PocoQuote();
                var increment = GetTimestampIncrement(totalCount, partitionCount);

                foreach (PocoQuote quote in q.All())
                {
                    GenerateQuoteValues(expected, increment, count);
                    AssertEqual(quote, expected);
                    count++;
                }

                Assert.That(count, Is.EqualTo(totalCount));
            }
        }

        private void AssertEqual(PocoQuote quote, PocoQuote expected)
        {
            Assert.That(quote.Timestamp, Is.EqualTo(expected.Timestamp));
            Assert.That(quote.Sym, Is.EqualTo(expected.Sym));
            Assert.That(quote.Mode, Is.EqualTo(expected.Mode));
            Assert.That(quote.Ex, Is.EqualTo(expected.Ex));
            Assert.That(quote.BidSize, Is.EqualTo(expected.BidSize));
            Assert.That(quote.Bid, Is.EqualTo(expected.Bid));
            Assert.That(quote.AskSize, Is.EqualTo(expected.AskSize));
            Assert.That(quote.Ask, Is.EqualTo(expected.Ask));
        }
    }
}