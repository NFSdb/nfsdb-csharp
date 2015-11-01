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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Queries;
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
        private IJournal<Trade> CreateJournal(EFileAccess access = EFileAccess.ReadWrite)
        {
            return Utils.CreateJournal<Trade>(access);
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
        public void Appends_out_of_order()
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

            trade.Timestamp -= 1;
            using (var journal = CreateJournal())
            {
                using (var wr = journal.OpenWriteTx())
                {
                    Assert.Throws<NFSdbInvalidAppendException>(() => wr.Append(trade));
                    wr.Commit();
                }
            }
        }


        [Test]
        [Ignore]
        public void ShouldWriteAfterTruncate()
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

                journal.Truncate();

                using (var wr = journal.OpenWriteTx())
                {
                    trade.Timestamp = DateUtils.DateTimeToUnixTimeStamp(DateTime.Now.AddDays(-1));
                    wr.Append(trade);
                    wr.Commit();
                }
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

        [Test]
        public void AddsFilePartsWhenReaderOpen()
        {
            var conf = Utils.ReadConfig<Trade>();
            conf.RecordHint = 100000;

            Utils.ClearJournal<Trade>();
            TimeSpan cleanLatency = TimeSpan.FromMilliseconds(10);
                // Change reader bit hints.
            var conf2 = conf;

            using (var journalReader = CreateJournal<Trade>(conf2, TimeSpan.FromSeconds(10), access: EFileAccess.Read))
            {
                IQuery<Trade> rdr = null;
                var trade = new Trade();

                var cancel = new CancellationTokenSource();
                var tnk = cancel.Token;
                var readTask = Task.Factory.StartNew(() =>
                {
                    while (!tnk.IsCancellationRequested)
                    {
                        using (rdr = journalReader.OpenReadTx())
                        {
                            var items = rdr.Items.Take(100).ToArray();
                            if (items.Length > 0)
                            {
                                Console.WriteLine("Max timestamp items " + items.Last().Timestamp);
                            }
                        }
                    }
                }, cancel.Token);

                const int batches = 1000;
                for (int i = 0; i < batches; i++)
                {
                    try
                    {
                        using (var journal = CreateJournal<Trade>(conf, cleanLatency, access: EFileAccess.ReadWrite))
                        {
                            using (var wr = journal.OpenWriteTx())
                            {
                                for (int j = 0; j < 1000; j++)
                                {
                                    trade.Timestamp = (i*batches + j)*3000L;
                                    wr.Append(trade);
                                }
                                wr.Commit();
                            }
                        }
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("Failed on iteration " + i);
                        throw;
                    }
                }
                cancel.Cancel();
            }
        }


        public static IJournal<T> CreateJournal<T>(JournalElement config, TimeSpan latency, EFileAccess access = EFileAccess.Read)
        {
            return new JournalBuilder(config)
                .WithAccess(access)
                .WithSerializerFactoryName(MetadataConstants.THRIFT_SERIALIZER_NAME)
                .WithServerTasksLatency(latency)
                .ToJournal<T>();
        }
    }
}