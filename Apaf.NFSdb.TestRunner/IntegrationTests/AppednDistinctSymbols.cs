using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.TestShared;
using Apaf.NFSdb.TestShared.Model;

namespace Apaf.NFSdb.TestRunner.IntegrationTests
{
    public class AppednDistinctSymbols : ITask
    {
        public void Run()
        {
            Utils.ClearJournal<Trade>();
            using (var journal = Utils.CreateJournal<Trade>(EFileAccess.ReadWrite))
            {
                using (var wr = journal.OpenWriteTx())
                {
                    var trade = new Trade();
                    for (int i = 0; i < 10E6; i++)
                    {
                        trade.Ex = "NYSE" + (char)(i % char.MaxValue);
                        wr.Append(trade);
                    }
                    wr.Commit();
                }
            }
        }

        public string Name { get { return "distinct-symbols"; } }
    }
}