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
using System.Globalization;
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Apaf.NFSdb.Tests.Tx;
using Moq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Columns
{
    [TestFixture]
    public class IndexColumnTests
    {
        private Mock<IRawFile> _symrr;
        private Mock<IRawFile> _symrk;

        [Test]
        public void Appends_value()
        {
            var tx = TestTxLog.TestContext();
            var id = CreateIndex(1000 * 16, tx);
            const int key = 53;
            
            // Act.
            var vals = Enumerable.Range(0, 1000).Select(i => (long)i);
            foreach (var val in vals)
            {
                id.Add(key, val, tx);
            }

            // Verify.
            var expected = string.Join("|",
                vals.Select(i => i.ToString(CultureInfo.InvariantCulture)));

            var result = string.Join("|", id.GetValues(key, tx).OrderBy(i => i)
                .Select(i => i.ToString(CultureInfo.InvariantCulture)));

            Assert.That(result, Is.EqualTo(expected));
        }


        [Test]
        public void Ucommited_values_are_not_visible()
        {
            var tx1 = TestTxLog.TestContext();
            var id = CreateIndex(1000 * 16, tx1);
            const int key = 71;
            id.Add(key, 0, tx1);
            Commit(tx1, 1);
            var result1 = string.Join("|", id.GetValues(key, tx1).OrderBy(i => i)
                .Select(i => i.ToString(CultureInfo.InvariantCulture)));

            Assert.That(result1, Is.EqualTo("0"));

            // Act.
            var pd = DeepClone(tx1);
            var vals = Enumerable.Range(1, 10).Select(i => (long)i);
            foreach (var val in vals)
            {
                id.Add(key, val, pd);
            }

            // Verify.
            var result = string.Join("|", id.GetValues(key, pd).OrderBy(i => i)
                .Select(i => i.ToString(CultureInfo.InvariantCulture)));

            Assert.That(result, Is.EqualTo("0"));
        }

        private static PartitionTxData DeepClone(PartitionTxData p)
        {
            var r = new PartitionTxData(p.AppendOffset.Length, p.PartitionID, p.StartDate, p.EndDate, new ReadContext())
            {
                LastTimestamp = p.LastTimestamp,
                NextRowID = p.NextRowID,
                IsPartitionUpdated = p.IsPartitionUpdated
            };

            for (int i = 0; i < p.AppendOffset.Length; i++)
            {
                r.AppendOffset[i] = p.AppendOffset[i];
                r.SymbolData[i] = p.SymbolData[i].DeepClone();
            }
            return r;
        }

        private void Commit(PartitionTxData pd, int rowcount)
        {
            var sd = pd.SymbolData[_symrk.Object.FileID];

            var keyBockOffset = sd.KeyBlockOffset;
            var keyBockSize = sd.KeyBlockSize;
            var symrAppOff = keyBockOffset + keyBockSize +
                MetadataConstants.K_FILE_KEY_BLOCK_HEADER_SIZE;

            _symrk.Object.WriteInt64(MetadataConstants.K_FILE_KEY_BLOCK_OFFSET, keyBockOffset);
            _symrk.Object.WriteInt64(keyBockOffset, keyBockSize);
            _symrk.Object.SetAppendOffset(symrAppOff);

            pd.AppendOffset[_symrk.Object.FileID] = symrAppOff;
            pd.NextRowID = rowcount;

            long symrAppendOffset = pd.AppendOffset[_symrr.Object.FileID];
            _symrr.Object.SetAppendOffset(symrAppendOffset);
        }

        private IndexColumn CreateIndex(int size, PartitionTxData pd)
        {
            int fileID = 0;
            _symrk = RawFileStub.InMemoryFile(size, fileID++);
            _symrr = RawFileStub.InMemoryFile(size, fileID);
            pd.AppendOffset[_symrk.Object.FileID] = 28;
            pd.SymbolData[_symrk.Object.FileID].KeyBlockOffset = 12;
            pd.NextRowID = long.MaxValue;

            return new IndexColumn(_symrk.Object, _symrr.Object, 100, 1000);
        }
    }
}