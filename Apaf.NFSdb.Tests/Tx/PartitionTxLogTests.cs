using System.IO;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Tx
{
    [TestFixture]
    public class PartitionTxLogTests
    {
        [TestCase(0)]
        [TestCase(43546431)]
        [TestCase(long.MaxValue)]
        public void CanReadWriteRowCount(long count)
        {
            var log = CreatePartitionTxLog();
            var file = CreateTempFile();

            log.WriteTxRecord(file, new PartitionTxRec
            {
                RowCount = count,
                VarLenFileAppendOffsets = new long[0]
            });

            // Erase
            var rec = new PartitionTxRec
            {
                RowCount = count + 10,
                VarLenFileAppendOffsets = new long[0]
            };

            // Act.
            log.ReadTxRecord(file, rec);

            Assert.That(rec.RowCount, Is.EqualTo(count));
        }

        private IRawFile CreateTempFile()
        {
            var fileName = Path.GetTempFileName();
            return new CompositeRawFile(fileName, 16, new CompositeFileFactory(), 
                EFileAccess.ReadWrite, -1, -1, -1, EDataType.Data);
        }

        private PartitionTxLog CreatePartitionTxLog()
        {
            return new PartitionTxLog();
        }
    }
}