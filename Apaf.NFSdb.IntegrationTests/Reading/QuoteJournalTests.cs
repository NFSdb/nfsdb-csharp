#region copyright
/*
 * Copyright (c) 2014. APAF http://apafltd.co.uk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Writes;
using Apaf.NFSdb.TestShared;
using Apaf.NFSdb.TestShared.Model;
using NUnit.Framework;

namespace Apaf.NFSdb.IntegrationTests.Reading
{
    [TestFixture]
    public class QuoteJournalTests
    {
        private static readonly double _bidRandom;
        private static readonly double _askRandom;
        private static readonly int _askSizeRandom;
        private static readonly int _bidSizeRandom;

        static QuoteJournalTests()
        {
            var random = new Random();
            _bidRandom = random.NextDouble();
            _askRandom = random.NextDouble();
            _askSizeRandom = random.Next();
            _bidSizeRandom = random.Next();
        }

        private const int GENERATE_RECORDS_COUNT = TestUtils.GENERATE_RECORDS_COUNT;
        private static readonly DateTime START = new DateTime(DateTime.Now.AddYears(-1).Year, 
            DateTime.Now.AddYears(-1).Month, 1);

        private static readonly long START_TIMESTAMP = DateUtils.DateTimeToUnixTimeStamp(START);

        public static readonly string[] SYMBOLS = new[]
        {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};

        private IJournal<Quote> CreateJournal(EFileAccess access = EFileAccess.ReadWrite)
        {
            return Utils.CreateJournal<Quote>(access);
        }

        [Test]
        [Category("Performance")]
        public void Should_read_all_rows()
        {
            var totalCount = (int)(5*GENERATE_RECORDS_COUNT);
            var elapsed = GenerateRecords(totalCount, 2);
            Console.WriteLine(elapsed);
            using (var j = CreateJournal(EFileAccess.Read))
            {
                var q = j.OpenReadTx();
                long count = 0;
                var sw = new Stopwatch();
                sw.Start();

                foreach (Quote quote in q.All())
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
        public void Opens_partitions_for_empty_journal()
        {
            Utils.ClearJournal<Quote>();
            using (var j = Utils.CreateJournal<Quote>(EFileAccess.Read))
            {
                const int totalCount = 300;
                GenerateRecords(totalCount, 3);

                var r2 = j.OpenReadTx();
                Assert.That(r2.PartitionCount, Is.EqualTo(4));
            }
        }

        [Test]
        public void Should_correctly_read_partition_counts()
        {
            GenerateRecords(2000, 5);

            using (var j = CreateJournal())
            {
                var q = j.OpenReadTx();
                Assert.That(q.All().Length.Value, Is.EqualTo(2000));
            }
        }

        [Test]
        public void Read_records_match_written_records()
        {
            const int partitionCount = 3;
            const int totalCount = (int) 2E6;
            GenerateRecords(totalCount, partitionCount);

            using (var j = CreateJournal())
            {
                var q = j.OpenReadTx();
                int i = 0;
                var increment = GetTimestampIncrement(totalCount, partitionCount);
                var writtenRec = new Quote();


                var sw = new Stopwatch();
                sw.Start();
                foreach (var quote in q.All())
                {
                    GenerateTradeValues(writtenRec, increment, i++);
                    Assert.That(quote.ToString(), Is.EqualTo(writtenRec.ToString()));
                }
                sw.Stop();
                Console.WriteLine(sw.Elapsed);
            }
        }

        [Test]
        public void Read_all_records_parallel()
        {
            const int partitionCount = 3;
            const int totalCount = (int)(6 * GENERATE_RECORDS_COUNT);
            // GenerateRecords(totalCount, partitionCount);

            using (var j = CreateJournal(EFileAccess.Read))
            {
                var q = j.OpenReadTx();
                var increment = GetTimestampIncrement(totalCount, partitionCount);
                var writtenRec = new Quote();
                var all =  q.All().ToRandomAccess();
                var part = Partitioner.Create(0, all.Length.Value);

                var sw = new Stopwatch();
                sw.Start();

                Parallel.ForEach(part,
                    (fromto, state, a) =>
                    {
                        for (long i = fromto.Item1; i < fromto.Item2; i++)
                        {
                            var num = all.Read((int)i);
                        }
                    });

                sw.Stop();
                Console.WriteLine(sw.Elapsed);                
            }
        }

        [Test]
        public void Read_records_reverse()
        {
            const int partitionCount = 3;
            const int totalCount = 100000;
            GenerateRecords(totalCount, partitionCount);


            var config = Utils.ReadConfig<Quote>();
            config.RecordHint = totalCount / 5;
            using (var j = Utils.CreateJournal<Quote>(config, EFileAccess.ReadWrite))
            {
                var q = j.OpenReadTx();
                var increment = GetTimestampIncrement(totalCount, partitionCount);
                var writtenRec = new Quote();
                var all = q.All().ToRandomAccess();
                for (long i =0; i < all.Length.Value; i++)
                {
                    all.Read((int)i);
                }
            }
        }
        
        [Test]
        public void Concurrent_writes_visible_to_readers()
        {
            const int partitionCount = 2;
            const int totalCount = 1000;
            GenerateRecords(totalCount, partitionCount);

            const int appendCount = 1000;
            var increment = GetTimestampIncrement(appendCount, partitionCount);
            var quote = new Quote();

            using (var writer = CreateJournal(EFileAccess.ReadWrite))
            using (var reader = CreateJournal(EFileAccess.Read))
            {
                for (int i = appendCount; i < appendCount + totalCount; i++)
                {
                    if (i == 1005)
                    {
                        
                    }
                    using (var wtx = writer.OpenWriteTx())
                    {
                        GenerateTradeValues(quote, increment, i);
                        wtx.Append(quote);
                        wtx.Commit();
                    }

                    if (i % 5 == 0)
                    {
                        var rtx = reader.OpenReadTx();
                        Assert.That(rtx.All().Length, Is.EqualTo(i + 1), i.ToString());
                        var last = rtx.All().Last();
                        Assert.That(quote.ToString(), Is.EqualTo(last.ToString()), i.ToString());
                    }
                }
            }
        }

        [Test]
        public void Supports_empty_commit()
        {
            var quote = new Quote();
            const int partitionCount = 2;
            const int appendCount = 1000;
            GenerateRecords(appendCount, partitionCount);
            var increment = GetTimestampIncrement(appendCount, partitionCount);
            int totalCommitedRecords = appendCount;

            using (var writer = CreateJournal(EFileAccess.ReadWrite))
            using (var reader = CreateJournal(EFileAccess.Read))
            {
                using (var wtx = writer.OpenWriteTx())
                {
                    for (int i = 0; i < 300; i++)
                    {
                        GenerateTradeValues(quote, increment, totalCommitedRecords + i);
                        wtx.Append(quote);
                    }
                    wtx.Commit();
                    totalCommitedRecords += 300;
                }

                using (var wtx = writer.OpenWriteTx())
                {
                    wtx.Commit();
                }

                var rtx = reader.OpenReadTx();
                Assert.That(rtx.All().Length, Is.EqualTo(totalCommitedRecords));
                var last = rtx.All().Last();
                Assert.That(quote.ToString(), Is.EqualTo(last.ToString()));
            }
        }

        [Test]
        public void Tumble_dry_transaction_test()
        {
            int seed = 3;
            var quote = new Quote();
            const int partitionCount = 2;
            const int iterationCount = 1000;
            const int appendCount = 10000;
            GenerateRecords(appendCount, partitionCount);
            var increment = GetTimestampIncrement(appendCount, partitionCount);

            var random = new Random(seed);
            int totalCommitedRecords = appendCount;

            using (var writer = CreateJournal(EFileAccess.ReadWrite))
            using (var reader = CreateJournal(EFileAccess.Read))
            {
                for (int n = 0; n < iterationCount; n++)
                {
                    var recorsToAppend = random.Next(0, 100);
                    var commitFlag = random.Next()%2 != 0;
                    using (var wtx = writer.OpenWriteTx())
                    {
                        for (int i = 0; i < recorsToAppend; i++)
                        {
                            GenerateTradeValues(quote, increment, totalCommitedRecords + i);
                            wtx.Append(quote);
                        }
                        if (commitFlag)
                        {
                            wtx.Commit();
                            totalCommitedRecords += recorsToAppend;
                        }
                    }

                    if (commitFlag && recorsToAppend != 0)
                    {
                        var errorString = string.Format("Total commited {0}, seed {1}, iteration {2}", totalCommitedRecords, seed, n);
                        var rtx = reader.OpenReadTx();
                        Assert.That(rtx.All().Length, Is.EqualTo(totalCommitedRecords), errorString);
                        var last = rtx.All().Last();
                        Assert.That(quote.ToString(), Is.EqualTo(last.ToString()), errorString);
                    }
                }
            }
        }

        public static TimeSpan GenerateRecords(int count, int partitionCount)
        {
            var increment = GetTimestampIncrement(count, partitionCount);
            Utils.ClearJournal<Quote>();
            var stopwatch = new Stopwatch();
            using (var journal = Utils.CreateJournal<Quote>(EFileAccess.ReadWrite))
            {
                stopwatch.Start();
                using (var wr = journal.OpenWriteTx())
                {
                    GenerateRecords(count, wr, increment);
                    wr.Commit();
                }
            }
            stopwatch.Stop();
            return stopwatch.Elapsed;
        }

        public static void GenerateRecords(int count, IWriter<Quote> wr, long increment)
        {
            var trade = new Quote();
            trade.Ex = "LXE";
            trade.Mode = "Fast trading";
            var timestamp = START_TIMESTAMP;
            var bidRandom = _bidRandom;
            var askRandom = _askRandom;
            var askSizeRandom = _askSizeRandom;
            var bidSizeRandom = _bidSizeRandom;

            for (int i = 0; i < count; i++)
            {
                trade.Timestamp = timestamp;
                timestamp += increment;

                trade.Bid = i * bidRandom;
                trade.Ask = i * askRandom;
                trade.AskSize = i % askSizeRandom;
                trade.BidSize = i % bidSizeRandom;
                trade.Sym = SYMBOLS[i%SYMBOLS.Length];

                wr.Append(trade);
            }
        }

        public static long GetTimestampIncrement(long count, int partitionCount)
        {
            return (long) ((START.AddMonths(partitionCount).AddDays(-1) - START).TotalMilliseconds/count);
        }

        public static void GenerateTradeValues(Quote trade, long incrementMs, long i)
        {
            trade.Timestamp = START_TIMESTAMP + incrementMs*i;
            trade.Bid = i * _bidRandom;
            trade.Ask = i * _askRandom;
            trade.AskSize = (int)(i % _askSizeRandom);
            trade.BidSize = (int)(i % _bidSizeRandom);
            trade.Ex = "LXE";
            trade.Mode = "Fast trading";
            trade.Sym = SYMBOLS[i%SYMBOLS.Length];
        }
    }
}