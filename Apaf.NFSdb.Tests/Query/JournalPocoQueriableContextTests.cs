using System;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Tests.Columns.PocoModel;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Query
{
    public class JournalPocoQueriableContextTests
    {
        [Test]
        public void Supports_period_or_symbol_and_timestamp()
        {
            string result =
                ExecuteLambda(
                    items => from q in items
                             where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                   || q.Sym == "Symbol_1") && q.Timestamp >= new DateTime(241)
                             select q, 1);

            Assert.That(result, Is.EqualTo("281,264,263,262,261,260,241"));
        }

        [Test]
        public void Supports_first()
        {
            string result =
                ExecuteLambdaSingle(
                    items => (from q in items
                              where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                    || q.Sym == "Symbol_1") && q.Timestamp >= new DateTime(241)
                              select q).First(), 1);

            Assert.That(result, Is.EqualTo("281"));
        }

        [Test]
        public void Supports_first_with_sub_where()
        {
            string result =
                ExecuteLambdaSingle(
                    items => (from q in items
                              where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                    || q.Sym == "Symbol_1")
                              select q).First(q => q.Timestamp <= new DateTime(241)), 1);

            Assert.That(result, Is.EqualTo("241"));
        }

        [Test]
        public void Throws_when_first_returns_empty_result()
        {
            Assert.Throws<InvalidOperationException>
                (() =>
                ExecuteLambdaSingle(
                    items => (from q in items
                              where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                    || q.Sym == "Symbol_1")
                              select q).First(q => q.Timestamp <= new DateTime(0)), 1));
        }

        [Test]
        public void Supports_first_or_default()
        {
            string result =
                ExecuteLambdaSingle(
                    items => (from q in items
                             where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                   || q.Sym == "Symbol_1") && q.Timestamp >= new DateTime(241)
                             select q).FirstOrDefault(), 1);

            Assert.That(result, Is.EqualTo("281"));
        }

        [Test]
        public void Supports_first_or_default_with_sub_where()
        {
            string result =
                ExecuteLambdaSingle(
                    items => (from q in items
                              where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                    || q.Sym == "Symbol_1")
                              select q).FirstOrDefault(q => q.Timestamp <= new DateTime(241)), 1);

            Assert.That(result, Is.EqualTo("241"));
        }

        [Test]
        public void Supports_first_or_default_with_null_result()
        {
            string result =
                ExecuteLambdaSingle(
                    items => (from q in items
                              where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                    || q.Sym == "Symbol_1")
                              select q).FirstOrDefault(q => q.Timestamp <= new DateTime(0)), 1);

            Assert.That(result, Is.Null);
        }

        [Test]
        public void Supports_last_or_default_with_sub_where()
        {
            string result =
                ExecuteLambdaSingle(
                    items => (from q in items
                              where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                    || q.Sym == "Symbol_1")
                              select q).LastOrDefault(q => q.Timestamp <= new DateTime(241)), 1);

            Assert.That(result, Is.EqualTo("1"));
        }


        [Test]
        public void Supports_last_with_sub_where()
        {
            string result =
                ExecuteLambdaSingle(
                    items => (from q in items
                              where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                    || q.Sym == "Symbol_1")
                              select q).Last(q => q.Timestamp <= new DateTime(241)), 1);

            Assert.That(result, Is.EqualTo("1"));
        }

        [TestCase("Symbol_1", 21, Result = "0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,22")]
        [TestCase("Symbol_2", 21, Result = "0,1,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21")]
        [TestCase("Symbol_N", 21, Result = "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20")]
        public string Supports_not_equal_symbol(string symbol, int take)
        {
            string result =
                ExecuteLambda(
                    items => (from q in items
                              where (q.Sym != symbol)
                              select q).OrderBy(q => q.Timestamp).Take(take), 1);

            return result;
        }

        [Test]
        public void Supports_not_null_equal_symbol()
        {
            var result =
                ExecuteLambdaQuotes(
                    items => (from q in items
                              where (q.Ex != null)
                              select q).OrderBy(q => q.Timestamp), 1);

            Assert.That(result.Any(r => r.Timestamp.Ticks % 20 == 2), Is.EqualTo(false));
        }

        [TestCase(1, 11, Result = "0,2,3,4,5,7,8,9,10,12,13")]
        [TestCase(2, 11, Result = "0,1,3,4,5,6,8,9,10,11,13")]
        [TestCase(-1, 11, Result = "0,1,2,3,4,5,6,7,8,9,10")]
        public string Supports_not_equal_non_symbol(int bidSize, int take)
        {
            string result =
                ExecuteLambda(
                    items => (from q in items
                              where (q.BidSize != bidSize)
                              select q).OrderBy(q => q.Timestamp).Take(take), 1);

            return result;
        }

        [Test]
        public void Supports_not_equal_null_non_symbol()
        {
            string result =
                ExecuteLambda(
                    items => (from q in items
                              where (q.Bid != null)
                              select q).OrderBy(q => q.Timestamp).Take(10), 1);

            Assert.That(result, Is.EqualTo("0,2,3,4,5,7,8,9,10,12"));
        }

        [Test]
        public void Supports_last()
        {
            string result =
                ExecuteLambdaSingle(
                    items => (from q in items
                              where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                    || q.Sym == "Symbol_1")
                              select q).Last(), 1);

            Assert.That(result, Is.EqualTo("1"));
        }

        [Test]
        public void Throws_when_last_returns_no_results()
        {
            Assert.Throws<InvalidOperationException>
                (() =>
                    ExecuteLambdaSingle(
                        items => (from q in items
                            where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                   || q.Sym == "Symbol_1")
                            select q).Last(q => q.Timestamp <= DateTime.MinValue), 1));
        }

        [Test]
        public void Supports_last_or_default_with_null_result()
        {
            string result =
                ExecuteLambdaSingle(
                    items => (from q in items
                              where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                    || q.Sym == "Symbol_1")
                              select q).LastOrDefault(q => q.Timestamp <= new DateTime(0)), 1);

            Assert.That(result, Is.Null);
        }

        [Test]
        public void Supports_single()
        {
            string result =
                ExecuteLambdaSingle(
                    items => (from q in items
                              where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                    || q.Sym == "Symbol_1") && q.Timestamp >= new DateTime(281)
                              select q).Single(), 1);

            Assert.That(result, Is.EqualTo("281"));
        }

        [Test]
        public void Supports_single_with_where()
        {
            string result =
                ExecuteLambdaSingle(
                    items => (from q in items
                              where ((q.Timestamp >= new DateTime(260) && q.Timestamp < new DateTime(265))
                                    || q.Sym == "Symbol_1")
                              select q).Single(q => q.Timestamp >= new DateTime(281)), 1);

            Assert.That(result, Is.EqualTo("281"));
        }

        private string ExecuteLambdaSingle(Func<IQueryable<DateTimeQuote>, DateTimeQuote> lambda, int increment = 2)
        {
            Utils.ClearJournal<DateTimeQuote>();
            var config = Utils.ReadConfig<DateTimeQuote>();
            config.SerializerName = MetadataConstants.POCO_SERIALIZER_NAME;

            using (var qj = CreateJournal<DateTimeQuote>(config, EFileAccess.ReadWrite))
            {
                AppendRecords(qj, 0, increment);
            }

            using (var qj = CreateJournal<DateTimeQuote>(config))
            {
                var rdr = qj.OpenReadTx();

                var qts = lambda(rdr.Items);

                // Verify.
                return qts == null ? null : qts.Timestamp.Ticks.ToString();
            }

        }

        private string ExecuteLambda(Func<IQueryable<DateTimeQuote>, IEnumerable<DateTimeQuote>> lambda,
            int increment = 2)
        {
            var qts = ExecuteLambdaQuotes(lambda, increment);

            // Act.
            var result = qts.Select(q => q.Timestamp.Ticks);

            // Verify.
            return string.Join(",", result);
        }


        private IEnumerable<DateTimeQuote> ExecuteLambdaQuotes(Func<IQueryable<DateTimeQuote>, IEnumerable<DateTimeQuote>> lambda, int increment = 2)
        {
            Utils.ClearJournal<DateTimeQuote>();
            var config = Utils.ReadConfig<DateTimeQuote>();
            config.SerializerName = MetadataConstants.POCO_SERIALIZER_NAME;
            
            using (var qj = CreateJournal<DateTimeQuote>(config, EFileAccess.ReadWrite))
            {
                AppendRecords(qj, 0, increment);
            }

            using (var qj = CreateJournal<DateTimeQuote>(config))
            {
                var rdr = qj.OpenReadTx();

                return lambda(rdr.Items).ToList();

            }
        }

        private IJournal<T> CreateJournal<T>(JournalElement config, EFileAccess readWrite = EFileAccess.Read)
        {
            return new JournalBuilder(config).WithAccess(readWrite).ToJournal<T>();
        }

        private static void AppendRecords(IJournal<DateTimeQuote> qj, long startDate, long increment)
        {
            using (var wr = qj.OpenWriteTx())
            {
                for (int i = 0; i < 300; i++)
                {
                    wr.Append(new DateTimeQuote
                    {
                        Ask = i,
                        Bid = i % 5 == 1 ? (double?)null : i%5,
                        BidSize = i % 5,
                        Ex = i % 20 == 2 ? null : "Ex_" + i % 20,
                        Sym = "Symbol_" + i % 20,
                        Timestamp = new DateTime(startDate + i * increment)
                    });
                }
                wr.Commit();
            }
        } 
    }
}