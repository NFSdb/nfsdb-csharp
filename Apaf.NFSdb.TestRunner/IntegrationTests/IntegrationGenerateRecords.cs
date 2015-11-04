using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.IntegrationTests;
using Apaf.NFSdb.TestShared;
using Apaf.NFSdb.TestShared.Model;

namespace Apaf.NFSdb.TestRunner.IntegrationTests
{
    public class IntegrationGenerateRecords : ITask
    {
        public string Name
        {
            get { return "integration-generate-null-records"; }
        }

        public void Run()
        {
            Utils.ClearJournal<Quote>(IntegrationTestConstants.NULL_RECORD_FOLDER_NAME);
            using (var j = Utils.CreateJournal<Quote>(EFileAccess.ReadWrite, IntegrationTestConstants.NULL_RECORD_FOLDER_NAME))
            {
                string[] symbols = IntegrationTestConstants.TEST_SYMBOL_LIST;
                var timestamp = IntegrationTestConstants.NULL_RECORD_FIRST_TIMESTAMP;
                long timestampIncr = IntegrationTestConstants.NULL_RECORD_TIMESTAMP_INCREMENT;

                using (var writer = j.OpenWriteTx())
                {
                    for (int i = 0; i < IntegrationTestConstants.NULL_RECORD_COUNT; i++)
                    {
                        var q = new Quote();
                        if (i % 7 != 0)
                        {
                            q.Sym = symbols[i % symbols.Length];
                        }
                        else
                        {
                            q.__isset.sym = false;
                        }

                        if (i % 11 != 0)
                        {
                            q.Ask = i * 22.98007E8;
                        }
                        else
                        {
                            q.__isset.ask = false;
                        }

                        if (i % 13 != 0)
                        {
                            q.Bid = i * 22.98007E-8;
                        }
                        else
                        {
                            q.__isset.bid = false;
                        }

                        if (i % 3 != 0)
                        {
                            q.AskSize = i;
                        }
                        else
                        {
                            q.__isset.askSize = false;
                        }

                        if (i % 5 != 0)
                        {
                            q.BidSize = i * 7;
                        }
                        else
                        {
                            q.__isset.bidSize = false;
                        }

                        if (i % 2 != 0)
                        {
                            q.Ex = "LXE";
                        }
                        else
                        {
                            q.__isset.ex = false;
                        }

                        if (i % 17 != 0)
                        {
                            q.Mode  = "Some interesting string with киррилица and special char" + (char)(i % char.MaxValue);
                        }
                        else
                        {
                            q.__isset.mode = false;
                        }

                        q.Timestamp = timestamp;
                        timestamp += timestampIncr;

                        writer.Append(q);
                    }

                    writer.Commit();
                }
            }
        }
    }
}