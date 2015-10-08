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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Query
{
    [TestFixture]
    public class JournalQueriableContextTests
    {
        [TestCase("Symbol_0", ExpectedResult = "280,260,240,220,200,180,160,140,120,100,80,60,40,20,0")]
        [TestCase("Symbol_14", ExpectedResult = "294,274,254,234,214,194,174,154,134,114,94,74,54,34,14")]
        public string Equal_symbol_filter(string value)
        {
            return ExecuteLambda(
                items => from q in items
                         where q.Sym == value
                         select q, 1);
        }

        [TestCase("Symbol_0", "Ex_0", ExpectedResult = "280,260,240,220,200,180,160,140,120,100,80,60,40,20,0")]
        [TestCase("Symbol_14", "Ex_14", ExpectedResult = "294,274,254,234,214,194,174,154,134,114,94,74,54,34,14")]
        [TestCase("Symbol_14", "Ex_0", ExpectedResult = "")]
        public string Symbol_filters_with_and_operator(string value, string exValue)
        {
            return ExecuteLambda(
                items => from q in items
                         where q.Sym == value && q.Ex == exValue
                         select q, 1);
        }

        [TestCase("Symbol_0", "Symbol_14", ExpectedResult = "294,280,274,260,254,240,234,220,214,200,194,180,174,160,154,140,134,120,114,100,94,80,74,60,54,40,34,20,14,0")]
        [TestCase("Symbol_1", "Symbol_12", ExpectedResult = "292,281,272,261,252,241,232,221,212,201,192,181,172,161,152,141,132,121,112,101,92,81,72,61,52,41,32,21,12,1")]
        public string Symbol_filters_with_or_operator(string value, string exValue)
        {
            return ExecuteLambda(
                items => from q in items
                         where q.Sym == value || q.Sym == exValue
                         select q, 1);
        }

        [TestCase("Symbol_0", "Symbol_14", "Ex_0", ExpectedResult = "280,260,240,220,200,180,160,140,120,100,80,60,40,20,0")]
        [TestCase("Symbol_1", "Symbol_12", "Ex_1", ExpectedResult = "281,261,241,221,201,181,161,141,121,101,81,61,41,21,1")]
        [TestCase("Symbol_1", "Symbol_12", "NoEx", ExpectedResult = "")]
        [TestCase("Symbol_3", "Symbol_12", "Ex_1", ExpectedResult = "")]
        public string Symbol_filters_with_or_and_and_operator(string value, string value2, string exValue)
        {
            return ExecuteLambda(
                items => from q in items
                         where (q.Sym == value || q.Sym == value2) && q.Ex == exValue
                         select q, 1);
        }

        [TestCase("Symbol_3,Symbol_12", ExpectedResult = "292,283,272,263,252,243,232,223,212,203,192,183,172,163,152,143,132,123,112,103,92,83,72,63,52,43,32,23,12,3")]
        [TestCase("Symbol_3,Symbol_132", ExpectedResult = "283,263,243,223,203,183,163,143,123,103,83,63,43,23,3")]
        [TestCase("", ExpectedResult = "")]
        [TestCase("alsdjfal", ExpectedResult = "")]
        public string Symbol_list_contains(string values)
        {
            var symbols = values.Split(',');
            return ExecuteLambda(
                items => from q in items
                         where symbols.Contains(q.Sym)
                         select q, 1);
        }

        [TestCase("Symbol_3,Symbol_12", "Ex_3,Ex_2", ExpectedResult = "283,263,243,223,203,183,163,143,123,103,83,63,43,23,3")]
        public string Multiple_symbol_list_contains(string symValues, string exValues)
        {
            var symbols = symValues.Split(',');
            var exs = exValues.Split(',');
            return ExecuteLambda(
                items => from q in items
                         where symbols.Contains(q.Sym) && exs.Contains(q.Ex)
                         select q, 1);
        }

        [TestCase("Symbol_3,Symbol_12", "Ex_3", "", ExpectedResult = "283,263,243,223,203,183,163,143,123,103,83,63,43,23,3")]
        [TestCase("Symbol_3,Symbol_12", "Ex_12", "Ex_3", ExpectedResult = "292,283,272,263,252,243,232,223,212,203,192,183,172,163,152,143,132,123,112,103,92,83,72,63,52,43,32,23,12,3")]
        [TestCase("Symbol_3,Symbol_12", "Ex_12", "", ExpectedResult = "292,272,252,232,212,192,172,152,132,112,92,72,52,32,12")]
        [TestCase("Symbol_3,Symbol_12", "", "", ExpectedResult = "")]
        public string Symbol_list_contains_and_other_symbols(string symbolValues, 
            string andEx, string andEx2)
        {
            var symbols = symbolValues.Split(',').ToList();

            return ExecuteLambda(
                items => from q in items
                         where symbols.Contains(q.Sym) && (q.Ex == andEx || q.Ex == andEx2)
                         select q, 1);
        }

        [TestCase("Symbol_3,Symbol_12", "Ex_3", ExpectedResult = "292,283,272,263,252,243,232,223,212,203,192,183,172,163,152,143,132,123,112,103,92,83,72,63,52,43,32,23,12,3")]
        [TestCase("Symbol_3", "Ex_12", ExpectedResult = "292,283,272,263,252,243,232,223,212,203,192,183,172,163,152,143,132,123,112,103,92,83,72,63,52,43,32,23,12,3")]
        [TestCase("Symbol_3", "Ex_12", ExpectedResult = "292,283,272,263,252,243,232,223,212,203,192,183,172,163,152,143,132,123,112,103,92,83,72,63,52,43,32,23,12,3")]
        [TestCase("Symbol_3", "", ExpectedResult = "283,263,243,223,203,183,163,143,123,103,83,63,43,23,3")]
        public string Symbol_list_contains_or_other_symbols(string symbolValues,
            string andEx)
        {
            var symbols = symbolValues.Split(',');
            return ExecuteLambda(
                items => from q in items
                    where symbols.Contains(q.Sym) || q.Ex == andEx
                    select q, 1);
        }

        [TestCase("Ex_0", ExpectedResult = "280,260,240,220,200,180,160,140,120,100,80,60,40,20,0")]
        public string Ex_symbol_list(string andEx)
        {
            return ExecuteLambda(
                items => from q in items
                         where q.Ex == andEx
                         select q, 1);
        }

        [TestCase("Symbol_3,Symbol_12", "Ex_3", "", ExpectedResult = "283,263,243,223,203,183,163,143,123,103,83,63,43,23,3")]
        [TestCase("Symbol_3,Symbol_12", "Ex_12", "Ex_3", ExpectedResult = "292,283,272,263,252,243,232,223,212,203,192,183,172,163,152,143,132,123,112,103,92,83,72,63,52,43,32,23,12,3")]
        [TestCase("Symbol_3,Symbol_12", "Ex_12", "", ExpectedResult = "292,272,252,232,212,192,172,152,132,112,92,72,52,32,12")]
        [TestCase("Symbol_3,Symbol_12", "", "", ExpectedResult = "")]
        public string Symbol_list_contains_with_more_than_5_values_or_other_symbols(string symbolValues,
            string andEx, string andEx2)
        {
            var symbols = symbolValues.Split(',').ToList();
            symbols.AddRange(new[] { "one", "two", "three", "four", "five", "six" });

            return ExecuteLambda(
                items => from q in items
                         where symbols.Contains(q.Sym) && (q.Ex == andEx || q.Ex == andEx2)
                         select q, 1);
        }

        [TestCase(594L, ExpectedResult = "598,596")]
        [TestCase(596L, ExpectedResult = "598")]
        [TestCase(597L, ExpectedResult = "598")]
        [TestCase(599L, ExpectedResult = "")]
        public string Supports_timestamp_greater_comparison(long timestamp)
        {
            return ExecuteLambda(
                items => from q in items
                         where q.Timestamp > timestamp
                         select q);
        }

        [TestCase(594L, ExpectedResult = "598,596,594")]
        [TestCase(596L, ExpectedResult = "598,596")]
        [TestCase(597L, ExpectedResult = "598")]
        [TestCase(598L, ExpectedResult = "598")]
        [TestCase(599L, ExpectedResult = "")]
        public string Supports_timestamp_greater_or_equal(long timestamp)
        {
            return ExecuteLambda(
                items => from q in items
                         where q.Timestamp >= timestamp
                         select q);
        }

        [TestCase(10L, ExpectedResult = "8,6,4,2,0")]
        [TestCase(5L, ExpectedResult = "4,2,0")]
        [TestCase(0L, ExpectedResult = "")]
        public string Supports_timestamp_less(long timestamp)
        {
            return ExecuteLambda(
                items => from q in items
                         where q.Timestamp < timestamp
                         select q);
        }

        [TestCase(10L, ExpectedResult = "10,8,6,4,2,0")]
        [TestCase(5L, ExpectedResult = "4,2,0")]
        [TestCase(0L, ExpectedResult = "0")]
        [TestCase(-1L, ExpectedResult = "")]
        public string Supports_timestamp_less_or_equal(long timestamp)
        {
            return ExecuteLambda(
                items => from q in items
                          where q.Timestamp <= timestamp
                          select q);
        }

        [TestCase(10L, 20L, ExpectedResult = "18,16,14,12,10")]
        [TestCase(0, 10L, ExpectedResult = "8,6,4,2,0")]
        public string Supports_timestamp_interval(long fromInclusive, long toExclusive)
        {
            return ExecuteLambda(
                items => from q in items
                    where q.Timestamp >= fromInclusive && q.Timestamp < toExclusive
                    select q);
        }

        [Test]
        public void Supports_multiple_periods()
        {
            string result =
                ExecuteLambda(
                         items => from q in items
                         where (q.Timestamp > 10 && q.Timestamp < 20)
                            || (q.Timestamp > 30 && q.Timestamp < 40)
                         select q);

            Assert.That(result, Is.EqualTo("38,36,34,32,18,16,14,12"));
        }

        [Test]
        public void Supports_multiple_periods_orders_correctly()
        {
            string result =
                ExecuteLambda(
                         items => from q in items
                                  where 
                                     (q.Timestamp > 30 && q.Timestamp < 40)||
                                     (q.Timestamp > 10 && q.Timestamp < 20)
                                  select q);

            Assert.That(result, Is.EqualTo("38,36,34,32,18,16,14,12"));
        }

        [Test]
        public void Supports_multiple_periods_and_symbol_orders_correctly()
        {
            string result =
                ExecuteLambda(
                         items => from q in items
                                  where
                                  ( (q.Timestamp > 30 && q.Timestamp < 40) ||
                                     (q.Timestamp > 10 && q.Timestamp < 20)
                                     ) && q.Sym == "Symbol_11"
                                  select q, 1);

            Assert.That(result, Is.EqualTo("31,11"));
        }

        [Test]
        public void Supports_multiple_periods_and_symbol_orders_correctly2()
        {
            string result =
                ExecuteLambda(
                         items => from q in items
                                  where
                                  ((q.Timestamp > 10 && q.Timestamp < 20)
                                  || (q.Timestamp > 30 && q.Timestamp < 40)
                                     ) && q.Sym == "Symbol_11"
                                  select q, 1);

            Assert.That(result, Is.EqualTo("31,11"));
        }

        [Test]
        public void Supports_multiple_periods_one_open()
        {
            string result =
                ExecuteLambda(
                         items => from q in items
                                  where (q.Timestamp > 10 && q.Timestamp < 20)
                                     || (q.Timestamp < 40)
                                  select q);

            Assert.That(result, Is.EqualTo("38,36,34,32,30,28,26,24,22,20,18,16,14,12,10,8,6,4,2,0"));
        }

        [Test]
        public void Supports_multiple_periods_and_symbol()
        {
            string result =
                ExecuteLambda(
                         items => from q in items
                                  where ((q.Timestamp >= 100 && q.Timestamp < 200)
                            || (q.Timestamp >= 300 && q.Timestamp < 400)) && q.Sym == "Symbol_1"
                                  select q, 1);

            Assert.That(result, Is.EqualTo("181,161,141,121,101"));
        }

        [Test]
        public void Supports_time_period_or_symbol()
        {
            string result =
                ExecuteLambda(
                    items => from q in items
                        where (q.Timestamp >= 260 && q.Timestamp < 265)
                              || q.Sym == "Symbol_1"
                        select q, 1);

            Assert.That(result, Is.EqualTo("281,264,263,262,261,260,241,221,201,181,161,141,121,101,81,61,41,21,1"));
        }

        [Test]
        public void Supports_period_or_symbol_and_timestamp()
        {
            string result =
                ExecuteLambda(
                    items => from q in items
                             where ((q.Timestamp >= 260 && q.Timestamp < 265)
                                   || q.Sym == "Symbol_1") && q.Timestamp >= 241
                             select q, 1);

            Assert.That(result, Is.EqualTo("281,264,263,262,261,260,241"));
        }

        [Test]
        public void Supports_timestamp_or_timestamps_or_symbol_and_timestamp()
        {
            string result =
                ExecuteLambda(
                    items => from q in items
                             where (q.Timestamp >= 290 || q.Sym == "Symbol_1" || q.Timestamp < 295) && q.Timestamp >= 281
                             select q, 1);

            Assert.That(result, Is.EqualTo("299,298,297,296,295,294,293,292,291,290,289,288,287,286,285,284,283,282,281"));

            var ints = result.Split(',').Select(int.Parse)
                .Where(i => !((i >=290 || i % 20 == 1 || i < 295) && i >= 281)).ToArray();
            Assert.That(string.Join(",", ints.Select(i => i.ToString(CultureInfo.InvariantCulture))), Is.Empty);
        }

        [TestCase(10L, 200L, "Symbol_0", ExpectedResult = "160,120,80,40")]
        public string Supports_timestamp_interval_and_symbol_search(long fromInclusive, long toExclusive, string symbol)
        {
            return ExecuteLambda(
                items => from q in items
                         where q.Timestamp >= fromInclusive && q.Timestamp < toExclusive
                           && q.Sym == symbol
                         select q);
        }

        [TestCase(10L, 200L, "Symbol_0", ExpectedResult = "40,80,120,160")]
        public string Supports_timestamp_interval_and_symbol_search_reverse(long fromInclusive, long toExclusive, string symbol)
        {
            return ExecuteLambda(
                items => (from q in items
                         where q.Timestamp >= fromInclusive && q.Timestamp < toExclusive
                           && q.Sym == symbol
                         select q).Reverse());
        }

        [TestCase(10L, ExpectedResult = "10")]
        [TestCase(30L, ExpectedResult = "30")]
        public string Supports_single_search(long fromInclusive)
        {
            return ExecuteLambda(
                items => items.Single(q => q.Timestamp == fromInclusive));
        }

        [TestCase(10, "Symbol_0", ExpectedResult = "1,1,1,1,1,2,2,2,2,2")]
        public string Supports_order_by_Bid(int take, string sym)
        {
            return ExecuteLambda(
                items => items.OrderByDescending(t => t.Timestamp)
                    .Where(it => it.Bid == 1.0 || it.Bid == 2.0).Take(take).OrderBy(t => t.Bid),
                quotes => quotes.Select(q => q.Bid));
        }

        private string ExecuteLambda(Func<IQueryable<Quote>, Quote> lambda, int increment = 2)
        {
            return ExecuteLambda(l => new[] { lambda(l) }, increment);
        }

        private string ExecuteLambda(Func<IQueryable<Quote>, IQueryable<Quote>> lambda, int increment = 2)
        {
            return ExecuteLambda(l => lambda(l).AsEnumerable(), increment);
        }


        private string ExecuteLambda(Func<IQueryable<Quote>, IEnumerable<Quote>> lambda, int increment = 2)
        {
            return ExecuteLambda(lambda, tt => tt.Select(q => q.Timestamp), increment);
        }

        private string ExecuteLambda<T>(Func<IQueryable<Quote>, IEnumerable<Quote>> lambda, Func<IEnumerable<Quote>, IEnumerable<T>> formatLambda, int increment = 2)
        {
            Utils.ClearJournal<Quote>();
            var config = Utils.ReadConfig<Quote>();

            using (var qj = Utils.CreateJournal<Quote>(config, EFileAccess.ReadWrite))
            {
                AppendRecords(qj, 0, increment);
            }

            using (var qj = Utils.CreateJournal<Quote>(config))
            {
                var rdr = qj.OpenReadTx();

                var qts = lambda(rdr.Items);

                // Act.
                var result = formatLambda(qts);

                // Verify.
                return string.Join(",", result);
            }

        }

        private static void AppendRecords(IJournal<Quote> qj, long startDate, long increment)
        {
            using (var wr = qj.OpenWriteTx())
            {
                for (int i = 0; i < 300; i++)
                {
                    wr.Append(new Quote
                    {
                        Ask = i,
                        Ex = "Ex_" + i %20,
                        Sym = "Symbol_" + i%20,
                        Bid = i % 5,
                        Timestamp = startDate + i*increment
                    });
                }
                wr.Commit();
            }
        }
    }
}