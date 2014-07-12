﻿using System;
using System.Diagnostics;
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Writes;
using Apaf.NFSdb.TestModel.Model;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.IntegrationTests.Writing
{
    [TestFixture]
    public class TradeJournalTests
    {
        private Journal<Trade> CreateJournal()
        {
            return Utils.CreateJournal<Trade>(EFileAccess.ReadWrite);
        }

        [Test]
        public void Appends_one_record()
        {
            Utils.ClearJournal<Trade>();

            var trade = new Trade
            {
                Cond = "BBL",
                Ex = "NYSE",
                Price = 345.09,
                Size = 300,
                Sym = "GOOG",
                Stop = 1,
                Timestamp = DateUtils.DateTimeToUnixTimeStamp(DateTime.Now)
            };

            using (var journal = CreateJournal())
            {
                using (var wr = journal.OpenWriteTx())
                {
                    wr.Append(trade);
                    wr.Commit();
                }
            }

            using (var readJournal = Utils.CreateJournal<Trade>())
            {
                var rd = readJournal.OpenReadTx();
                var readTrade = rd.All().Single();
                Assert.That(readTrade.ToString(), Is.EqualTo(trade.ToString()));
            }
        }

        [Test]
        public void Appends_null_symbol()
        {
            Utils.ClearJournal<Trade>();

            var trade = new Trade
            {
                Cond = "BBL",
                Ex = "NYSE",
                Price = 345.09,
                Size = 300,
                Sym = null,
                Stop = 1,
                Timestamp = DateUtils.DateTimeToUnixTimeStamp(DateTime.Now)
            };

            using (var journal = CreateJournal())
            {
                using (var wr = journal.OpenWriteTx())
                {
                    wr.Append(trade);
                    wr.Commit();
                }
            }

            using (var readJournal = Utils.CreateJournal<Trade>())
            {
                var rd = readJournal.OpenReadTx();
                var readTrade = rd.All().Single();
                Assert.That(readTrade.ToString(), Is.EqualTo(trade.ToString()));
            }
        }

        [Test]
        [Category("Performance")]
        public void Append_1M_trades_speed()
        {
            Utils.ClearJournal<Trade>();
            using (var journal = CreateJournal())
            {

                using (var wr = journal.OpenWriteTx())
                {
                    var symbols = new[]
                    {
                        "AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"
                    };
                    var start = DateTime.Now.AddYears(-1);

                    var stopwatch = Stopwatch.StartNew();
                    var trade = new Trade();
                    for (int i = 0; i < 1E6; i++)
                    {
                        trade.Timestamp = DateUtils.DateTimeToUnixTimeStamp(start.AddSeconds(i*10));
                        trade.Cond = "BBLSD";
                        trade.Size = i;
                        trade.Ex = "NYSE";
                        trade.Price = i*1.0001;
                        trade.Stop = 2*i;
                        trade.Sym = symbols[i%symbols.Length];
                        wr.Append(trade);
                    }
                    wr.Commit();

                    Console.WriteLine(stopwatch.Elapsed);
                }
            }
        }

        [Test]
        [ExpectedException(typeof(NFSdbLowAddressSpaceException))]
        public void AppendsManyDistinctSymbols()
        {
            Utils.ClearJournal<Trade>();
            using (var journal = CreateJournal())
            {
                using (var wr = journal.OpenWriteTx())
                {

                    var trade = new Trade();
                    for (int i = 0; i < 1E6; i++)
                    {
                        trade.Ex = "NYSE" + (char)(i % char.MaxValue);
                        wr.Append(trade);
                    }
                    wr.Commit();
                }
            }
        }
    }
}