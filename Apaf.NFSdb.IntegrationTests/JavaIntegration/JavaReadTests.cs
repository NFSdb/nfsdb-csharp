using System.Globalization;
using System.Linq;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.TestModel.Model;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.IntegrationTests.JavaIntegration
{
    [TestFixture]
    public class JavaReadTests
    {
        [Test]
        public void Records_with_nulls()
        {
            using (var j = Utils.CreateJournal<Quote>(EFileAccess.Read, 
                IntegrationTestConstants.NULL_RECORD_FOLDER_NAME))
            {
                var read = j.OpenReadTx();
                long i = 0;
                string[] symbols = IntegrationTestConstants.TEST_SYMBOL_LIST;
                var timestamp = IntegrationTestConstants.NULL_RECORD_FIRST_TIMESTAMP;
                long timestampIncr = IntegrationTestConstants.NULL_RECORD_TIMESTAMP_INCREMENT;
                foreach (Quote quote in read.All())
                {
                    var q = quote;
                    Assert.That(q.__isset.sym, Is.EqualTo(i % 7 != 0), "Sym null check. Record " + i);
                    Assert.That(q.__isset.ask, Is.EqualTo(i % 11 != 0), "Ask null check. Record " + i);
                    Assert.That(q.__isset.bid, Is.EqualTo(i % 13 != 0), "Bid null check. Record " + i);
                    Assert.That(q.__isset.askSize, Is.EqualTo(i % 3 != 0), "AskSize null check. Record " + i);
                    Assert.That(q.__isset.bidSize, Is.EqualTo(i % 5 != 0), "BidSize null check. Record " + i);
                    Assert.That(q.__isset.ex, Is.EqualTo(i % 2 != 0), "Ex null check. Record " + i);
                    Assert.That(q.__isset.mode, Is.EqualTo(i % 17 != 0), "Mode null check. Record " + i);

                    if (q.__isset.sym)
                    {
                        Assert.That(q.Sym, Is.EqualTo(symbols[i % symbols.Length]), "Sym failed for rec " + i);
                    }

                    if (q.__isset.ask)
                    {
                        Assert.That(q.Ask, Is.EqualTo(i * 22.98007E8), "Ask failed for rec " + i);
                    }

                    if (q.__isset.bid)
                    {
                        Assert.That(q.Bid, Is.EqualTo(i * 22.98007E-8), "Bid failed for rec " + i);
                    }

                    if (q.__isset.askSize)
                    {
                        Assert.That(q.AskSize, Is.EqualTo(i), "AskSize failed for rec " + i);
                    }

                    if (q.__isset.bidSize)
                    {
                        Assert.That(q.BidSize, Is.EqualTo(i*7), "BidSize failed for rec " + i);
                    }

                    if (q.__isset.ex)
                    {
                        Assert.That(q.Ex, Is.EqualTo("LXE"), "Ex failed for rec " + i);
                    }

                    if (q.__isset.mode)
                    {
                        Assert.That(q.Mode, Is.EqualTo(
                            "Some interesting string with киррилица and special char" + (char)(i % char.MaxValue)
                            ), "Mode failed for rec " + i);
                    }

                    Assert.That(q.Timestamp, Is.EqualTo(timestamp + timestampIncr * i), 
                        "Timestamp failed for rec " + i);
                    i++;
                }

                Assert.That(i, Is.EqualTo(IntegrationTestConstants.NULL_RECORD_COUNT));
            }
        }

        [Test]
        public void Records_with_nulls_indexes()
        {
            using (var j = Utils.CreateJournal<Quote>(EFileAccess.Read, 
                IntegrationTestConstants.NULL_RECORD_FOLDER_NAME))
            {
                var read = j.OpenReadTx();
                string[] symbols = IntegrationTestConstants.TEST_SYMBOL_LIST;

                for (int i = 0; i < symbols.Length; i++)
                {
                    int symIndex = i;

                    var rowIndex =
                        (from q in read.Items
                            where q.Sym == symbols[symIndex]
                            select q).AsEnumerable()
                            .Select(q => (q.Timestamp - IntegrationTestConstants.NULL_RECORD_FIRST_TIMESTAMP)
                                         / IntegrationTestConstants.NULL_RECORD_TIMESTAMP_INCREMENT).ToArray();

                    for (int l = 0; l < rowIndex.Length; l++)
                    {
                        var index = rowIndex[l];
                        if (index % 7 != 0)
                        {
                            Assert.That(index % symbols.Length, Is.EqualTo(symIndex),
                                string.Format("Symbol: {0}, row {1}", symbols[symIndex], index));
                        }
                    }

                    if (symIndex <= 7)
                    {
                        Assert.That(rowIndex.Length, Is.EqualTo(234), symIndex.ToString(CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        Assert.That(rowIndex.Length, Is.EqualTo(233), symIndex.ToString(CultureInfo.InvariantCulture));
                    }
                }
            }
        }


    }
}
