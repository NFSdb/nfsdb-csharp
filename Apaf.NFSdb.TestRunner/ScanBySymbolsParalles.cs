#region copyright
/*
 * Copyright (c) 2014. APAF (Alex Pelagenko).
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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.IntegrationTests.Reading;
using Apaf.NFSdb.TestModel.Model;
using Apaf.NFSdb.TestShared;

namespace Apaf.NFSdb.TestRunner
{
    public class ScanBySymbolsParalles : ITask
    {
        public void Run()
        {
            using (var j = Utils.CreateJournal<Quote>(EFileAccess.Read))
            {
                int delay = 0;
                Parallel.ForEach(QuoteJournalTests.SYMBOLS,
                    sym =>
                    {
                        var currentDealy = Interlocked.Increment(ref delay);
                        Thread.Sleep(TimeSpan.FromSeconds(currentDealy * 10));
                        IQuery<Quote> q = j.OpenReadTx();

                        var symbolQuotes = from qq in q.Items
                                           where qq.Sym == sym
                                           select qq;
                        int count = 0;

                        var sw = new Stopwatch();
                        sw.Start();
                        Quote current;
                        foreach (Quote quote in symbolQuotes)
                        {
                            current = quote;
                            count++;
                        }
                        sw.Stop();
                        Console.WriteLine("Sym: {0}, Elaplsed: {1}, Count: {2}", sym, sw.Elapsed, count);
                    });
            }
        }

        public string Name
        {
            get { return "scan-by-symbols-parallel"; }
        }
    }
}