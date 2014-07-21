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
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Writes;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Query
{
    [TestFixture]
    public class QueryTests
    {
        [TestCase("Symbol_1", 300, ExpectedResult = "281,261,241,221,201,181,161,141,121,101,81,61,41,21,1")]
        [TestCase("Symbol_0", 300, ExpectedResult = "280,260,240,220,200,180,160,140,120,100,80,60,40,20,0")]
        [TestCase("Symbol_12", 10, ExpectedResult = "292,272,252,232,212,192,172,152,132,112,92,72,52,32,12")]
        public string Rows_by_symbol_with_record_hint(string sym, int recordHint)
        {
            Utils.ClearJournal<Quote>();
            var config = Utils.ReadConfig<Quote>();
            config.RecordHint = recordHint;

            using (var qj = Utils.CreateJournal<Quote>(config, EFileAccess.ReadWrite))
            {
                AppendRecords(qj, 0, 1);
                var rdr = qj.OpenReadTx();

                // Act.
                var result = rdr.AllBySymbol("Sym", sym).Select(q => q.Ask);

                // Verify.
                return string.Join(",", result);
            }
        }

        [TestCase("Symbol_1", 300, ExpectedResult = "281,261,241,221,201,181,161,141,121,101,81,61,41,21,1")]
        [TestCase("Symbol_0", 300, ExpectedResult = "280,260,240,220,200,180,160,140,120,100,80,60,40,20,0")]
        [TestCase("Symbol_12", 10, ExpectedResult = "292,272,252,232,212,192,172,152,132,112,92,72,52,32,12")]
        [TestCase("no_sym", 10, ExpectedResult = "")]
        public string Rows_by_symbol_multiple_partitions(string sym, int recordHint)
        {
            Utils.ClearJournal<Quote>();
            var config = Utils.ReadConfig<Quote>();
            config.PartitionType = EPartitionType.Day;
            config.RecordHint = recordHint;
            const int records = 300;
            var startDate = DateUtils.DateTimeToUnixTimeStamp(DateTime.Now.AddYears(-1).Date);
            var increment = 3 * (long)TimeSpan.FromDays(1).TotalMilliseconds / records;

            using (var qj = Utils.CreateJournal<Quote>(config, EFileAccess.ReadWrite))
            {
                AppendRecords(qj, startDate, increment);
                Assert.That(qj.Partitions.Count(), Is.GreaterThan(1));
                var rdr = qj.OpenReadTx();
                
                // Act.
                var result = rdr.AllBySymbol("Sym", sym).Select(q => q.Ask);

                // Verify.
                return string.Join(",", result);
            }
        }

        [TestCase("Symbol_12", 0, 4, ExpectedResult = "292,272,252,232,212,192,172,152,132,112,92,72,52,32,12")]
        [TestCase("Symbol_12", 1, 4, ExpectedResult = "292,272,252,232,212,192,172,152,132,112")]
        [TestCase("Symbol_12", 2, 4, ExpectedResult = "292,272,252,232,212")]
        [TestCase("Symbol_12", 3, 4, ExpectedResult = "")]
        [TestCase("Symbol_0", 1, 2, ExpectedResult = "180,160,140,120,100")]
        [TestCase("Symbol_0", 1, 3, ExpectedResult = "280,260,240,220,200,180,160,140,120,100")]
        [TestCase("Symbol_8", 1, 1, ExpectedResult = "")]
        public string Rows_by_symbol_over_interval(string sym, int fromDay, int toDay)
        {
            Utils.ClearJournal<Quote>();
            var config = Utils.ReadConfig<Quote>();
            config.PartitionType = EPartitionType.Day;
            const int records = 300;
            var startDate = DateUtils.DateTimeToUnixTimeStamp(DateTime.Now.AddYears(-1).Date);
            var millisecsPerday = (long) TimeSpan.FromDays(1).TotalMilliseconds;
            var increment = 3 * millisecsPerday / records;

            using (var qj = Utils.CreateJournal<Quote>(config, EFileAccess.ReadWrite))
            {
                AppendRecords(qj, startDate, increment);
                Assert.That(qj.Partitions.Count(), Is.GreaterThan(1));
                var rdr = qj.OpenReadTx();
                var fromDate = DateUtils.UnixTimestampToDateTime(startDate + fromDay * millisecsPerday);
                var toDate = DateUtils.UnixTimestampToDateTime(startDate + toDay * millisecsPerday);

                // Act.
                var result = rdr.AllBySymbolValueOverInterval("Sym", 
                    sym, new DateInterval(fromDate, toDate)).Select(q => q.Ask);

                // Verify.
                return string.Join(",", result);
            }
        }

        private static void AppendRecords(Journal<Quote> qj, long startDate, long increment)
        {
            using (var wr = qj.OpenWriteTx())
            {
                for (int i = 0; i < 300; i++)
                {
                    wr.Append(new Quote
                    {
                        Ask = i,
                        Sym = "Symbol_" + i%20,
                        Timestamp = startDate + i*increment
                    });
                }
                wr.Commit();
            }
        }
    }
}