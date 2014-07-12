using System;
using System.Diagnostics;
using Apaf.NFSdb.IntegrationTests.Reading;
using Apaf.NFSdb.TestModel.Model;
using Apaf.NFSdb.TestShared;

namespace Apaf.NFSdb.TestRunner
{
    public class AppendRecords : ITask
    {
        public void Run()
        {
            Utils.ClearJournal<Quote>();
            const int partitionCount = 1;
            const int totalCount = (int) 30E6;

            var sw1 = new Stopwatch();
            sw1.Start();
            QuoteJournalTests.GenerateRecords(totalCount, partitionCount);
            sw1.Stop();
            Console.WriteLine(sw1.Elapsed);
        }

        public string Name
        {
            get { return "append-records"; }
        }
    }
}