﻿#region copyright
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
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Apaf.NFSdb.Tests.Storage;
using Apaf.NFSdb.Tests.Tx;
using NUnit.Framework;
using Moq;

namespace Apaf.NFSdb.Tests.Columns
{
    [TestFixture]
    public class SymbolMapColumnTests
    {
        private Mock<IRawFile> _symTestD;
        private Mock<IRawFile> _symTestR;
        private Mock<IRawFile> _symTestK;
        private IRawFile _symd;
        private Mock<IRawFile> _symi;
        private Mock<IRawFile> _symrk;
        private Mock<IRawFile> _symrr;
        private SymbolCache _symbolCatch;

        [Test]
        public void Appends_record()
        {
            var tx = TestTxLog.TestContext();
            var sym = CreateSymbolColumn(tx);
            
            // Act.
            sym.SetValue(0, "First Value", tx);
            _symbolCatch.Reset();

            // Verify.
            var result = sym.GetValue(0, tx.ReadCache);
            Assert.That(result, Is.EqualTo("First Value"));
        }

        [TestCase("1|1|2|3|4|5|567|1|879|098", "1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1")]
        public void Add_distinct_values_only_to_symbo_file(string firstValueSet, string repeatedVals)
        {
            var tx = TestTxLog.TestContext();
            var sym = CreateSymbolColumn(tx);

            int rowId = 0;
            int writeCount = 0;
            // Prepare.
            _symrr.Setup(s => s.WriteInt64(It.IsAny<long>(), It.IsAny<long>()))
                .Callback((long a, long b) => writeCount++);

            foreach (string val in firstValueSet.Split('|'))
            {
                sym.SetValue(rowId++, val, tx);
            }
            _symbolCatch.Reset();

            // Act.
            foreach (string val in repeatedVals.Split('|'))
            {
                sym.SetValue(rowId++, val, tx);
            }

            // Verify.
            _symrr.Verify(s => s.WriteInt64(It.IsAny<long>(), It.IsAny<long>()),
                Times.Exactly(writeCount));

        }

        private SymbolMapColumn CreateSymbolColumn(PartitionTxData tx)
        {
            int fileID = 0;
            _symTestD =  RawFileStub.InMemoryFile(4096, fileID++);
            _symTestK = RawFileStub.InMemoryFile(4096, fileID++);
            _symTestR = RawFileStub.InMemoryFile(4096, fileID++);
            _symd = new BufferBinaryReader(new byte[4096], fileID++);
            _symi = RawFileStub.InMemoryFile(4096, fileID++);
            _symrk = RawFileStub.InMemoryFile(4096, fileID++);
            _symrr = RawFileStub.InMemoryFile(4096, fileID);
            _symbolCatch = new SymbolCache();
            var smc = new SymbolMapColumn(
                0,
                0,
                data: _symTestD.Object,
                datak: _symTestK.Object,
                datar:_symTestR.Object,
                symd:_symd,
                symi:_symi.Object,
                symk:_symrk.Object,
                symr: _symrr.Object, 
                propertyName: "symTest",
                capacity: 24,
                recordCountHint: 100,
                maxLen: 56);

            tx.SymbolData[_symTestK.Object.FileID].KeyBlockOffset = 16;
            tx.AppendOffset[_symTestK.Object.FileID] = 28;
            tx.SymbolData[_symrk.Object.FileID].KeyBlockOffset = 16;
            tx.AppendOffset[_symrk.Object.FileID] = 28;

            return smc;
        }
    }
}