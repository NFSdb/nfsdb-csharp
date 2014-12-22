using System.Collections.Generic;
using System.IO;
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Tests.Common;
using Moq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Storage
{
    [TestFixture]
    public class FileTxSupportTests
    {
        [TestCase("i.d-8|s.d-8")]
        public void Should_revert_append_offset_on_commit_failure(string fileNameOffset)
        {
            var files = CreateFileMocks(fileNameOffset, "s.d");
            var ftx = CreateFileTxSupport(files);
            var tx = CreateNewTxContext(files.Count);

            // Non failing d file.
            var fileMock = FindFile(files, "i.d");
            var file = fileMock.Object;
            var originalOffset = file.GetAppendOffset();

            // Update append offset..
            tx.GetPartitionTx(0).AppendOffset[file.FileID] = 10;

            try
            {
                // Act.
                ftx.Commit(tx);
            }
            catch (NFSdbCommitFailedException)
            {
                // Check append offset reverted.
                fileMock.Verify(f => f.SetAppendOffset(It.IsAny<long>()), Times.Exactly(2));
                Assert.That(file.GetAppendOffset(), Is.EqualTo(originalOffset));

                return;
            }
            Assert.Fail("Expected exception is not thrown");
        }

        [TestCase("i.d-8|s.k-4|s.d-8")]
        public void Should_revert_keybock_offset_on_commit_failure(string fileNameOffset)
        {
            var files = CreateFileMocks(fileNameOffset, "s.d");
            var ftx = CreateFileTxSupport(files);
            var tx = CreateNewTxContext(files.Count);

            // K file.
            var fileMock = FindFile(files, "s.k");
            fileMock.Setup(f => f.DataType).Returns(EDataType.Datak);
            var file = fileMock.Object;
            var symbol = tx.GetPartitionTx(0).SymbolData[file.FileID];
            var originalOffset = file.ReadInt64(MetadataConstants.K_FILE_KEY_BLOCK_OFFSET);

            // Update keybock.
            symbol.KeyBlockOffset = 10;

            try
            {
                // Act.
                ftx.Commit(tx);
            }
            catch (NFSdbCommitFailedException)
            {
                // Check keyblock reverted.
                fileMock.Verify(f => f.WriteInt64(MetadataConstants.K_FILE_KEY_BLOCK_OFFSET,
                    It.IsAny<long>()), Times.Exactly(2));

                Assert.That(file.ReadInt64(MetadataConstants.K_FILE_KEY_BLOCK_OFFSET),
                    Is.EqualTo(originalOffset));       

                return;
            }
            Assert.Fail("Expected exception is not thrown");
        }

        private Mock<IRawFile> FindFile(IEnumerable<Mock<IRawFile>> storage, string name)
        {
            return storage.First(f => f.Object.Filename.EndsWith(name));
        }

        private ITransactionContext CreateNewTxContext(int fileCount)
        {
            var tx = new TransactionContext(fileCount);
            var pd = new PartitionTxData(fileCount);

            tx.AddPartition(pd, 0);
            return tx;
        }

        private FileTxSupport CreateFileTxSupport(List<Mock<IRawFile>> files)
        {
            var jornalMeta = new Mock<IJournalMetadataCore>();
            jornalMeta.Setup(j => j.FileCount).Returns(files.Count);

            var storage = new Mock<IColumnStorage>();
            storage.Setup(f => f.AllOpenedFiles()).Returns(files.Select(f => f.Object));

            return new FileTxSupport(0, storage.Object, jornalMeta.Object);
        }

        private static List<Mock<IRawFile>> CreateFileMocks(string fileNameOffset, string failName)
        {
            var files = TestUtils.SplitNameSize(fileNameOffset);
            var fileMocks = new List<Mock<IRawFile>>();
            int fileID = 0;

            foreach (var file in files)
            {
                var fileMock = new Mock<IRawFile>();
                long appendOffset = file.Value;
                var buffer = new BufferBinaryReader(new byte[1024]);

                fileMock.Setup(f => f.GetAppendOffset()).Returns(appendOffset);
                if (file.Key == failName)
                {
                    fileMock.Setup(f => f.SetAppendOffset(It.IsAny<long>())).Throws(new IOException());
                }
                else
                {
                    fileMock.Setup(f => f.SetAppendOffset(It.IsAny<long>())).Callback((long v) => appendOffset = v);
                }
                fileMock.Setup(f => f.ReadInt64(It.IsAny<long>())).Returns((long offset) => buffer.ReadInt64(offset));
                fileMock.Setup(f => f.WriteInt64(It.IsAny<long>(), It.IsAny<long>()))
                    .Callback((long offset, long val) => buffer.WriteInt64(offset, val));

                fileMock.Setup(f => f.Filename).Returns(file.Key);
                fileMock.Setup(f => f.FileID).Returns(fileID++);

                fileMocks.Add(fileMock);
            }
            return fileMocks;
        }
    }
}