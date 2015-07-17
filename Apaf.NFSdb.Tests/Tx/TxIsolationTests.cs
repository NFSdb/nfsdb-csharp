using System;
using System.Linq;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Tx
{
    [TestFixture]
    public class TxIsolationTests
    {
        [Test]
        public void ShouldNotReturnUncommitedIndexValues()
        {
            const int increment = 2;
            Utils.ClearJournal<Quote>();
            var config = Utils.ReadConfig<Quote>();

            using (var qj = Utils.CreateJournal<Quote>(config, EFileAccess.ReadWrite))
            {
                using (var wr = qj.OpenWriteTx())
                {

                    for (int i = 0; i < 300; i++)
                    {
                        wr.Append(new Quote
                        {
                            Ex = "Ex_" + i%20,
                            Sym = "Symbol_" + i%20,
                            Timestamp = i*increment
                        });
                    }
                    wr.Commit();
                }

                using (var wr = qj.OpenWriteTx())
                {
                    for (int i = 0; i < 300; i++)
                    {
                        wr.Append(new Quote
                        {
                            Ex = "Ex_" + i%20,
                            Sym = "Symbol_" + i%20,
                            Timestamp = 299*2
                        });

                        using (var rdr = qj.OpenReadTx())
                        {
                            // Act.
                            var qts = rdr.AllBySymbolValueOverInterval("Sym", "Symbol_0", DateInterval.Any).ToArray();

                            Assert.That(qts.Length, Is.EqualTo(15));
                        }
                    }
                }
            }
        }

        [Test]
        public void ShouldNotReturnUncommitedMasterIndexValues()
        {
            const int increment = 2;
            Utils.ClearJournal<Quote>();
            var config = Utils.ReadConfig<Quote>();

            using (var qj = Utils.CreateJournal<Quote>(config, EFileAccess.ReadWrite))
            {
                using (var wr = qj.OpenWriteTx())
                {

                    for (int i = 0; i < 300; i++)
                    {
                        wr.Append(new Quote
                        {
                            Ex = "Ex_" + i%20,
                            Sym = "Symbol_" + i%20,
                            Timestamp = i*increment
                        });
                    }
                    wr.Commit();
                }

                using (var wr = qj.OpenWriteTx())
                {
                    for (int i = 0; i < 300; i++)
                    {
                        wr.Append(new Quote
                        {
                            Ex = "Ex_" + i%20,
                            Sym = "Symbol_" + i%20,
                            Timestamp = 299*2
                        });

                        using (var rdr = qj.OpenReadTx())
                        {
                            // Act.
                            var qts = rdr.AllBySymbolValueOverInterval("Sym", "Symbol_21", DateInterval.Any).ToArray();

                            Assert.That(qts.Length, Is.EqualTo(0));
                        }
                    }
                }
            }
        }


        [Test]
        public void ShouldNotReturnCorrectRowsAfterRollback()
        {
            const int increment = 2;
            Utils.ClearJournal<Quote>();
            var config = Utils.ReadConfig<Quote>();
            var symSymbol = config.Symbols.Single(s => s.Name.EndsWith("sym", StringComparison.OrdinalIgnoreCase));
            symSymbol.HintDistinctCount = 20;
            config.RecordHint = 20;

            using (var qj = Utils.CreateJournal<Quote>(config, EFileAccess.ReadWrite))
            {
                using (var wr = qj.OpenWriteTx())
                {

                    for (int i = 0; i < 300; i++)
                    {
                        wr.Append(new Quote
                        {
                            Sym = "Symbol_" + i % 20,
                            Timestamp = i * increment
                        });
                    }
                    wr.Commit();
                }

                using (var wr = qj.OpenWriteTx())
                {
                    for (int i = 0; i < 300; i++)
                    {
                        wr.Append(new Quote
                        {
                            Sym = "Symbol_" + i % 20,
                            Timestamp = 299 * 2
                        });

                        using (var rdr = qj.OpenReadTx())
                        {
                            // Act.
                            var qts = rdr.AllBySymbolValueOverInterval("Sym", "Symbol_21", DateInterval.Any).ToArray();

                            Assert.That(qts.Length, Is.EqualTo(0));
                        }
                    }
                }
            }
        }
    }
}