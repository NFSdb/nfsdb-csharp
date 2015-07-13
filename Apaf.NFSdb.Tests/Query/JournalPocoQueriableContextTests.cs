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

        private string ExecuteLambda(Func<IQueryable<DateTimeQuote>, IEnumerable<DateTimeQuote>> lambda, int increment = 2)
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

                // Act.
                var result = qts.Select(q => q.Timestamp.Ticks);

                // Verify.
                return string.Join(",", result);
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
                        Ex = "Ex_" + i % 20,
                        Sym = "Symbol_" + i % 20,
                        Timestamp = new DateTime(startDate + i * increment)
                    });
                }
                wr.Commit();
            }
        } 
    }
}