using System;
using System.IO;
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Storage.Serializer.Records;
using Apaf.NFSdb.TestShared;
using Apaf.NFSdb.TestShared.Model;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Ql
{
    [TestFixture]
    public class QlThriftNullTests
    {
        private const string FOLDER_PATH = "ThriftNullTests";

        [TestCase("Symbol_0", 20, ExpectedResult = "4,3,2,1,,4,3,2,1,,4,3,2,1,,4,3,2,1,")]
        public string ShouldReadNullsThriftSerializer(string symbol, int take)
        {
            return
                ExecuteQuery<int>(
                    "SELECT ToP @top AskSize FROM Quote",
                    new QlParameter("top", take)
                    );
        }

        [TestCase("=", 10, ExpectedResult = "0,5,10,15,20,25,30,35,40,45")]
        [TestCase("IS", 10, ExpectedResult = "0,5,10,15,20,25,30,35,40,45")]
        [TestCase("IS NOT", 10, ExpectedResult = "1,2,3,4,6,7,8,9,11,12")]
        [TestCase("<>", 10, ExpectedResult = "1,2,3,4,6,7,8,9,11,12")]
        public string ShouldSelectNullsPocoSerializer(string column, int take)
        {
            return
                ExecuteQuery<long>(
                    string.Format("SELECT ToP @top Timestamp FROM Quote " +
                                  " WHERE AskSize {0} NULL Order by Timestamp", column),
                    new QlParameter("top", take));
        }

        private string ExecuteQuery<T>(string query, params QlParameter[] parameters) where T : struct
        {
            return ExecuteQuery(query,
                tt => string.Join(",", tt.RecordIDs().Select(rowid => tt.GetNullable<T>(rowid, 0)))
                , parameters);
        }

        private static IJournal<Quote> OpenJournal(EFileAccess access)
        {
            string directoryPath = Path.Combine(Utils.FindJournalsPath(), FOLDER_PATH);

            return new JournalBuilder()
                .WithRecordCountHint((int)1E6)
                .WithPartitionBy(EPartitionType.Month)
                .WithLocation(directoryPath)
                .WithSymbolColumn("Sym", 20, 5, 5)
                .WithTimestampColumn("Timestamp")
                .WithAccess(access)
                .WithSerializerFactoryName(MetadataConstants.THRIFT_SERIALIZER_NAME)
                .ToJournal<Quote>();
        }

        private string ExecuteQuery(string query, Func<IRecordSet, string> formatLambda, params QlParameter[] parameters)
        {
            Utils.ClearJournal<Quote>(FOLDER_PATH);
            string location;

            using (var qj = OpenJournal(EFileAccess.ReadWrite))
            {
                location = qj.Metadata.Settings.DefaultPath;
                AppendRecords(qj, 0, 1);
            }

            using (var qj = new JournalBuilder()
                .WithAccess(EFileAccess.Read)
                .WithLocation(location)
                .ToJournal())
            {
                using (var rdr = qj.OpenRecordReadTx())
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
                    var quote = new Quote
                    {
                        Ask = i,
                        Ex = "Ex_" + i % 20,
                        Sym = "Symbol_" + i % 20,
                        Bid = i % 5,
                        BidSize = -i,
                        AskSize = i % 5,
                        Timestamp = startDate + i * increment
                    };
                    if (i % 5 == 0)
                    {
                        quote.__isset.askSize = false;
                    }
                    wr.Append(quote);
                }
                wr.Commit();
            }
        }
    }
}