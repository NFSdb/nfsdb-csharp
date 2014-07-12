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