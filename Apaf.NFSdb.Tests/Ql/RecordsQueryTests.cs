using System;
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Queries.Records;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Ql
{
    [TestFixture]
    public class RecordsQueryTests
    {
        [TestCase("Symbol_0", ExpectedResult = "280,260,240,220,200,180,160,140,120,100,80,60,40,20,0")]
        [TestCase("Symbol_14", ExpectedResult = "294,274,254,234,214,194,174,154,134,114,94,74,54,34,14")]
        public string Equal_symbol_filter(string value)
        {
            return ExecuteQuery("SELECT FROM Quote WHERE Sym = '" + value + "'");
        }

        private string ExecuteQuery(string query, int increment = 1)
        {
            return ExecuteQuery(query,
                tt => string.Join(",", tt.Map(new[] {"Timestamp"}).RecordIDs().Select(rowid => tt.Get<long>(rowid, 0)))
                , increment);
        }

        private string ExecuteQuery(string query, Func<IRecordSet, string> formatLambda, int increment = 1)
        {
            Utils.ClearJournal<Quote>();
            var config = Utils.ReadConfig<Quote>();

            using (var qj = Utils.CreateJournal<Quote>(config, EFileAccess.ReadWrite))
            {
                AppendRecords(qj, 0, increment);
            }

            using (var qj = Utils.CreateJournal<Quote>(config))
            {
                using (var rdr = qj.Core.OpenRecordReadTx())
                {
                    var qts = rdr.Execute(query);

                    // Act.
                    var result = formatLambda(qts);

                    // Verify.
                    return string.Join(",", result);
                }
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
                        Ex = "Ex_" + i % 20,
                        Sym = "Symbol_" + i % 20,
                        Bid = i % 5,
                        Timestamp = startDate + i * increment
                    });
                }
                wr.Commit();
            }
        }
    }
}