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
using System.Diagnostics;
using Apaf.NFSdb.Core.Storage;
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
            const int totalCount = (int) 20e6;
            Utils.ClearJournal<Quote>();
            const int increment = 1000;
            var sw1 = new Stopwatch();
            const int count = 2;
            const int startIndex = -2;

            for (int i = startIndex; i < count; i++)
            {
                Utils.ClearJournal<Quote>();
                using (var journal = Utils.CreateJournal<Quote>(EFileAccess.ReadWrite))
                {
                    using (var wr = journal.OpenWriteTx())
                    {
                        if (i == 0)
                        {
                            Console.WriteLine(DateTime.Now);
                            sw1.Start();
                        }
                        QuoteJournalTests.GenerateRecords(totalCount, wr, increment);
                        wr.Commit();
                    }
                }

            }
            sw1.Stop();
            Console.WriteLine(DateTime.Now);
            Console.WriteLine(sw1.Elapsed);
            Console.WriteLine(sw1.Elapsed.TotalMilliseconds/count);
        }

        public string Name
        {
            get { return "append-records"; }
        }
    }
}