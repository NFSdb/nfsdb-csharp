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
using System.Linq;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Tests.Tx
{
    public class TestTxLog
    {
        public static TransactionContext TestContext()
        {
            const int partitions = 100;
            const int files = 100;
            var paritionTx = Enumerable.Range(0, partitions)
                .Select(p => new PartitionTxData()
                {
                    AppendOffset = new long[files],
                    SymbolData = Enumerable.Range(0, files)
                        .Select(f => new SymbolTxData()).ToArray()
                }).ToArray();

            var tx = new TransactionContext(100, paritionTx);
            return tx;
        } 
    }
}