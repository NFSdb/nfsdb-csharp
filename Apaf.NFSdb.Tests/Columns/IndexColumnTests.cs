using System;
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
            id.Add(key, 278, tx1);
            Commit(tx1);
            var result1 = string.Join("|", id.GetValues(key, tx1).OrderBy(i => i)
                .Select(i => i.ToString(CultureInfo.InvariantCulture)));

            Assert.That(result1, Is.EqualTo("278"));

            // Act.
            var tx = new TransactionContext(tx1);
            var vals = Enumerable.Range(0, 10).Select(i => (long)i);
            foreach (var val in vals)
            {
                id.Add(key, val, tx);
            }
            tx = new TransactionContext(tx1);

            // Verify.
            var result = string.Join("|", id.GetValues(key, tx).OrderBy(i => i)
                .Select(i => i.ToString(CultureInfo.InvariantCulture)));

            Assert.That(result, Is.EqualTo("278"));
        }

        private void Commit(TransactionContext tx1)
        {
            var pd = tx1.PartitionTx[_symrk.Object.PartitionID];
            var sd = pd.SymbolData[_symrk.Object.FileID];

            var keyBockOffset = sd.KeyBlockOffset;
            var keyBockSize = sd.KeyBlockSize;
            var symrAppOff = keyBockOffset + keyBockSize +
                MetadataConstants.K_FILE_KEY_BLOCK_HEADER_SIZE;

            _symrk.Object.WriteInt64(MetadataConstants.K_FILE_KEY_BLOCK_OFFSET, keyBockOffset);
            _symrk.Object.WriteInt64(keyBockOffset, keyBockSize);
            _symrk.Object.SetAppendOffset(symrAppOff);

            pd.AppendOffset[_symrk.Object.FileID] = symrAppOff;

            long symrAppendOffset = pd.AppendOffset[_symrr.Object.FileID];
            _symrr.Object.SetAppendOffset(symrAppendOffset);
        }

        private IndexColumn CreateIndex(int size, TransactionContext tx)
        {
            int fileID = 0;
            _symrk = RawFileStub.InMemoryFile(size, fileID++);
            _symrr = RawFileStub.InMemoryFile(size, fileID);
            PartitionTxData pd = tx.PartitionTx[_symrk.Object.PartitionID];
            pd.AppendOffset[_symrk.Object.FileID] = 28;
            pd.SymbolData[_symrk.Object.FileID].KeyBlockOffset = 12;
            return new IndexColumn(_symrk.Object, _symrr.Object, 100, 1000);
        }
    }
}