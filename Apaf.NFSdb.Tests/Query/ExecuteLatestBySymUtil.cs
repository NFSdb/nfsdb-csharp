using System;
using System.Linq;
using System.Linq.Expressions;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Query
{
    public class ExecuteLatestBySymUtil
    {
        public static string ExecuteLambda(Func<IQueryable<Quote>, IQueryable<Quote>> lambda, 
            int increment, bool createNulls = false)
        {
            return ExecuteLambda(lambda, increment, q => q.Sym, createNulls);
        }

        public static string ExecuteLambda<TKey>(Func<IQueryable<Quote>, IQueryable<Quote>> lambda,
            int increment, Expression<Func<Quote, TKey>> latestBy, bool createNulls = false)
        {
            Utils.ClearJournal<Quote>();
            var config = Utils.ReadConfig<Quote>();
            var indexed = ExecuteLamdaOnJournal(lambda, increment, config, createNulls);

            config.Columns = config.Columns.Where(s =>
                !string.Equals("Sym", s.Name, StringComparison.OrdinalIgnoreCase)).ToList();

            Utils.ClearJournal<Quote>();
            //var nonIndexed = ExecuteLamdaOnJournal(lambda, increment, config, latestBy, createNulls);

            //Assert.That(nonIndexed, Is.EqualTo(indexed), "Unindexed version does not equal to indexed version");
            return indexed;
        }

        private static string ExecuteLamdaOnJournal(Func<IQueryable<Quote>, IQueryable<Quote>> lambda,
            int increment,
            JournalElement config, bool createNulls = false)
        {
            return ExecuteLamdaOnJournal(lambda, increment, config, q => q.Sym, createNulls);
        }

        private static string ExecuteLamdaOnJournal<TKey>(Func<IQueryable<Quote>, IQueryable<Quote>> lambda, int increment,
            JournalElement config, Expression<Func<Quote, TKey>> latestBy, bool createNulls = false)
        {
            using (var qj = Utils.CreateJournal<Quote>(config, EFileAccess.ReadWrite))
            {
                AppendRecords(qj, 0, increment, createNulls);
                var rdr = qj.OpenReadTx();

                var qts = lambda(rdr.Items.LatestBy(latestBy));

                // Act.
                var result = qts.AsEnumerable().Select(q => q.Timestamp);

                // Verify.
                return string.Join(",", result);
            }
        }

        private static void AppendRecords(IJournal<Quote> qj, long startDate, long increment, bool createNulls = false)
        {
            using (var wr = qj.OpenWriteTx())
            {
                for (int i = 0; i < 300; i++)
                {
                    wr.Append(new Quote
                    {
                        Ask = i,
                        AskSize = i % 20,
                        Ex = createNulls && i%20 == 0 ? null : "Ex_" + i % 20,
                        Sym = createNulls && i%20 == 0 ? null : "Symbol_" + i % 20,
                        Timestamp = startDate + i * increment
                    });
                }
                wr.Commit();
            }
        } 
    }
}