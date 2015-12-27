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
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Ql
{
    [TestFixture]
    public class QlPocoNullTests
    {
        private const string FolderPath = "NullableQuote";

        public class NullableQuote
        {
            public long Timestamp { get; set; }
            public string Sym { get; set; }
            public double Bid { get; set; }
            public double? Ask { get; set; }
            public int BidSize { get; set; }
            public int? AskSize { get; set; }
            public string Mode { get; set; }
            public string Ex { get; set; }
        }

        [TestCase("Symbol_0", 20, ExpectedResult = "4,3,2,1,,4,3,2,1,,4,3,2,1,,4,3,2,1,")]
        public string ShouldReadNullsPocoSerializer(string symbol, int take)
        {
            return
                ExecuteQuery<int>(
                    "SELECT ToP @top AskSize FROM Quote",
                    MetadataConstants.POCO_SERIALIZER_NAME,
                    new QlParameter("top", take),
                    new QlParameter("sym", symbol)
                    );
        }

        private string ExecuteQuery<T>(string query, string serializerName, params QlParameter[] parameters) where T : struct
        {
            return ExecuteQuery(query, 
                tt => string.Join(",", tt.RecordIDs().Select(rowid => tt.GetNullable<T>(rowid, 0)))
                , serializerName, parameters);
        }

        private static IJournal<NullableQuote> OpenJournal(EFileAccess access, string serializerName)
        {
            string directoryPath = Path.Combine(Utils.FindJournalsPath(), FolderPath);

            return new JournalBuilder()
                .WithRecordCountHint((int)1E6)
                .WithPartitionBy(EPartitionType.Month)
                .WithLocation(directoryPath)
                .WithSymbolColumn("Sym", 20, 5, 5)
                .WithTimestampColumn("Timestamp")
                .WithAccess(access)
                .WithSerializerFactoryName(serializerName)
                .ToJournal<NullableQuote>();
        }

        private string ExecuteQuery(string query, Func<IRecordSet, string> formatLambda, string serializer, params QlParameter[] parameters)
        {
            Utils.ClearJournal<NullableQuote>(FolderPath);
            string location;

            using (var qj = OpenJournal(EFileAccess.ReadWrite, serializer))
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

        private static void AppendRecords(IJournal<NullableQuote> qj, long startDate, long increment)
        {
            using (var wr = qj.OpenWriteTx())
            {
                for (int i = 0; i < 300; i++)
                {
                    wr.Append(new NullableQuote
                    {
                        Ask = i,
                        Ex = "Ex_" + i % 20,
                        Sym = "Symbol_" + i % 20,
                        Bid = i % 5,
                        BidSize = -i,
                        AskSize = (i % 5 == 0) ? (int?)null : i%5,
                        Timestamp = startDate + i * increment
                    });
                }
                wr.Commit();
            }
        }
    }
}