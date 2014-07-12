using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Writes;
using Apaf.NFSdb.TestModel.Model;
using Apaf.NFSdb.TestShared;
using Microsoft.Concurrency.TestTools.UnitTesting.Chess;
using NUnit.Framework;

namespace Apaf.NFSdb.ConcurrencyTests
{
    [TestFixture]
    public class JournalTests
    {
        private static readonly DateTime START = new DateTime(DateTime.Now.AddYears(-1).Year,
            DateTime.Now.AddYears(-1).Month, 1);

        private static readonly long START_TIMESTAMP =
            DateUtils.DateTimeToUnixTimeStamp(START);

        private static readonly string[] SYMBOLS =
        {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};

        [Test]
        [ChessTestMethod]
        public void Concurrent_reads_from_multiple_partitions()
        {
            Utils.ClearJournal<Quote>();
            using (var j = Utils.CreateJournal<Quote>(EFileAccess.Read))
            {
                const int totalCount = 10;
                GenerateRecords(totalCount, 2);

                var tsk1 = Task.Factory.StartNew(() =>
                {
                    var r1 = j.OpenReadTx();
                    Assert.That(j.Partitions.Count(), Is.EqualTo(2));
                });

                
                var r2 = j.OpenReadTx();
                Assert.That(j.Partitions.Count(), Is.EqualTo(2));

                tsk1.Wait();
            }
        }

        public static void GenerateRecords(int count, int partitionCount)
        {
            var increment = GetTimestampIncrement(count, partitionCount);
            using (var journal = Utils.CreateJournal<Quote>(EFileAccess.ReadWrite))
            {
                using (var wr = journal.OpenWriteTx())
                {
                    var stopwatch = Stopwatch.StartNew();
                    var trade = new Quote();
                    for (int i = 0; i < count; i++)
                    {
                        GenerateTradeValues(trade, increment, i);
                        wr.Append(trade);
                    }
                    wr.Commit();

                    Console.WriteLine(stopwatch.Elapsed);
                }
            }
        }

        public static void GenerateTradeValues(Quote trade, long incrementMs, int i)
        {
            trade.Timestamp = START_TIMESTAMP + incrementMs*i;
            trade.Bid = i*2.04;
            trade.Bid = i;
            trade.Ask = i*50.09014;
            trade.AskSize = i;
            trade.Ex = "LXE";
            trade.Mode = "Fast trading";
            trade.Sym = SYMBOLS[i%SYMBOLS.Length];
        }

        public static long GetTimestampIncrement(int count, int partitionCount)
        {
            return (long) ((START.AddMonths(partitionCount).AddDays(-1) - START).TotalMilliseconds/count);
        }
    }
}