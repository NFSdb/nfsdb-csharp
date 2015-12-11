using System;
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Queries.Queryable;
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
            return ExecuteQuery("SELECT * FROM Quote WHERE Sym = '" + value + "'");
        }

        [TestCase("Symbol_0", ExpectedResult = "280,260,240,220,200,180,160,140,120,100,80,60,40,20,0")]
        [TestCase("Symbol_14", ExpectedResult = "294,274,254,234,214,194,174,154,134,114,94,74,54,34,14")]
        public string Equal_symbol_filter_as_param(string value)
        {
            return ExecuteQuery("SELECT * FROM Quote WHERE Sym = @s", new QlParameter("s", value));
        }

        [TestCase("Symbol_0", 260, ExpectedResult = "280,260")]
        [TestCase("Symbol_14", 225, ExpectedResult = "294,274,254,234")]
        public string Equal_symbol_filter_as_param_with_timestamp_restriction(string value, long fromTimestamp)
        {
            return ExecuteQuery("SELECT * FROM Quote WHERE Sym = @s and Timestamp >= @fromTimestamp",
                new QlParameter("s", value), new QlParameter("fromTimestamp", fromTimestamp));
        }

        [TestCase("Symbol_0,Symbol_14", 260, ExpectedResult = "294,280,274,260")]
        public string In_symbol_filter_as_param_with_timestamp_restriction(string values, long fromTimestamp)
        {
            return ExecuteQuery("SELECT * FROM Quote WHERE Sym in @vals and Timestamp >= @fromTimestamp",
                new QlParameter("vals", values.Split(new[] {','})),
                new QlParameter("fromTimestamp", fromTimestamp));
        }

        [TestCase("Symbol_0,Symbol_14", 260, ExpectedResult = "294,280,274,260")]
        public string In_symbol_filter_as_param_with_timestamp_restriction_with_brackets(string values, long fromTimestamp)
        {
            return ExecuteQuery("SELECT * FROM Quote WHERE Sym in ( @val1, @val2 ) and Timestamp >= @fromTimestamp",
                new QlParameter("val1", values.Split(new[] { ',' })[0]),
                new QlParameter("val2", values.Split(new[] { ',' })[1]),
                new QlParameter("fromTimestamp", fromTimestamp));
        }

        [TestCase("Symbol_0", "Symbol_14", 260, ExpectedResult = "294,280,274,260")]
        public string In_symbol_filter(string value1, string value2, long fromTimestamp)
        {
            return ExecuteQuery(
                string.Format("SELECT * FROM Quote WHERE Sym in ('{0}', '{1}') and Timestamp >= @fromTimestamp", value1, value2),
                new QlParameter("fromTimestamp", fromTimestamp));
        }

        [TestCase("Symbol_0", 0, 10, ExpectedResult = "280,260,240,220,200,180,160,140,120,100")]
        [TestCase("Symbol_0", 10, 10, ExpectedResult = "80,60,40,20,0")]
        [TestCase("Symbol_0", 1, 1, ExpectedResult = "260")]
        public string Skip_take(string value1, int skip, int take)
        {
            return ExecuteQuery(string.Format("SELECT ToP @top OFFSET @skip * FROM Quote WHERE Sym ='{0}'", value1),
                new QlParameter("top", take),
                new QlParameter("skip", skip)
                );
        }

        [TestCase("Timestamp", "asc", 0, 3, ExpectedResult = "0,20,40")]
        [TestCase("Timestamp", "desc", 0, 4, ExpectedResult = "280,260,240,220")]
        [TestCase("BidSize", "", 0, 4, ExpectedResult = "280,260,240,220")]
        [TestCase("BidSize", "desc", 10, 4, ExpectedResult = "200,220,240,260")]
        public string Order_by(string column, string direction, int skip, int take)
        {
            return
                ExecuteQuery(
                    string.Format("SELECT ToP @top OFFSET @skip * FROM Quote WHERE Sym ='Symbol_0' Order by {0} {1}",
                        column, direction),
                    new QlParameter("top", take),
                    new QlParameter("skip", skip)
                    );
        }

        [TestCase("Timestamp", "asc", 0, 3, ExpectedResult = "280,281,282")]
        [TestCase("Timestamp", "desc", 0, 4, ExpectedResult = "299,298,297,296")]
        [TestCase("BidSize", "", 0, 4, ExpectedResult = "299,298,297,296")]
        [TestCase("BidSize", "desc", 0, 4, ExpectedResult = "280,281,282,283")]
        public string Latest_by_sym(string column, string direction, int skip, int take)
        {
            return
                ExecuteQuery(
                    string.Format("SELECT ToP @top OFFSET @skip * FROM Quote Latest By Sym Order by {0} {1}",
                        column, direction),
                    new QlParameter("top", take),
                    new QlParameter("skip", skip)
                    );
        }

        [TestCase("Timestamp", 4, ExpectedResult = "299,298,297,296")]
        [TestCase("Abracadabra", 4, ExpectedException = typeof(NFSdbQueryableNotSupportedException),
            ExpectedMessage = "line 1:16 Column [Abracadabra] does not exists in journal Apaf.NFSdb.TestModel.Model.Quote")]
        public string SelectedLongColumns(string column, int take)
        {
            return SelectedColumns<long>(column, take);
        }

        [TestCase("Sym", 5, ExpectedResult = "Symbol_19,Symbol_18,Symbol_17,Symbol_16,Symbol_15")]
        public string SelectedStringColumns(string column, int take)
        {
            return SelectedColumns<string>(column, take);
        }

        [TestCase("Bid", 6, ExpectedResult = "4,3,2,1,0,4")]
        public string SelectedDoubleColumns(string column, int take)
        {
            return SelectedColumns<double>(column, take);
        }

        private string SelectedColumns<T>(string column, int take)
        {
            return
                ExecuteQuery(
                    string.Format("SELECT ToP @top {0} FROM Quote",
                        column),
                    r => string.Join(",", r.RecordIDs().Select(rowId => r.Get<T>(rowId, 0))),
                    new QlParameter("top", take)
                    );
        }

        private string ExecuteQuery(string query, params QlParameter[] parameters)
        {
            return ExecuteQuery(query,
                tt => string.Join(",", tt.Map(new[] {"Timestamp"}).RecordIDs().Select(rowid => tt.Get<long>(rowid, 0)))
                , parameters);
        }

        private string ExecuteQuery(string query, Func<IRecordSet, string> formatLambda, params QlParameter[] parameters)
        {
            Utils.ClearJournal<Quote>();
            var config = Utils.ReadConfig<Quote>();

            using (var qj = Utils.CreateJournal<Quote>(config, EFileAccess.ReadWrite))
            {
                AppendRecords(qj, 0, 1);
            }

            using (var qj = Utils.CreateJournal<Quote>(config))
            {
                using (var rdr = qj.Core.OpenRecordReadTx())
                {
                    var qts = rdr.Execute(query, parameters);

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
                        BidSize = -i,
                        Timestamp = startDate + i * increment
                    });
                }
                wr.Commit();
            }
        }
    }
}