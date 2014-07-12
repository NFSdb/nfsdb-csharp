using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Tests.Core;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Tx
{
#if !OPTIMIZE

    [TestFixture]
    public class TxLogTests
    {
        [TestCase(3L)]
        [TestCase(0L)]
        [TestCase(long.MaxValue)]
        public void Should_save_max_row_id(long maxRec)
        {
            var txLog = CreateTxLog();
            var rec = new TxRec
            {
                JournalMaxRowID = maxRec
            };

            txLog.Create(rec);
            var rec2 = txLog.Get();

            Assert.That(rec2.JournalMaxRowID, Is.EqualTo(maxRec));
        }

        [TestCase(123983093L)]
        [TestCase(0L)]
        [TestCase(long.MaxValue)]
        public void Should_save_last_timestamp(long timestamp)
        {
            var txLog = CreateTxLog();
            var rec = new TxRec
            {
                LastPartitionTimestamp = timestamp
            };

            txLog.Create(rec);
            var rec2 = txLog.Get();

            Assert.That(rec2.LastPartitionTimestamp, Is.EqualTo(timestamp));
        }

        [TestCase(123983093L)]
        [TestCase(0L)]
        [TestCase(long.MaxValue)]
        public void Should_save_LagSize(long lagSize)
        {
            var txLog = CreateTxLog();
            var rec = new TxRec
            {
                LagSize = lagSize
            };

            txLog.Create(rec);
            var rec2 = txLog.Get();

            Assert.That(rec2.LagSize, Is.EqualTo(lagSize));
        }

        [TestCase(null)]
        [TestCase("")]
        [TestCase("laganama")]
        public void Should_save_LagName(string name)
        {
            var txLog = CreateTxLog();
            var rec = new TxRec
            {
                LagName = name
            };

            txLog.Create(rec);
            var rec2 = txLog.Get();

            Assert.That(rec2.LagName, Is.EqualTo(name));
        }

        [TestCase("")]
        [TestCase("1,2,3")]
        [TestCase("40279981,40279982,4027998")]
        public void Should_save_SymbolTableSizes(string tableSizes)
        {
            var txLog = CreateTxLog();
            var symbols = string.IsNullOrEmpty(tableSizes) ? 
                null : tableSizes.Split(',').Select(int.Parse).ToArray();

            var rec = new TxRec
            {
                SymbolTableSizes = symbols
            };

            txLog.Create(rec);
            var rec2 = txLog.Get();

            var result = string.Join(",", rec2.SymbolTableSizes);
            Assert.That(result, Is.EqualTo(tableSizes));
        }

        [TestCase("")]
        [TestCase("40279981,40279982,4027998")]
        [TestCase("1,2,3")]
        public void Should_save_SymbolTableIndexPointers(string tableSizes)
        {
            var txLog = CreateTxLog();
            var symbols = string.IsNullOrEmpty(tableSizes) ?
                null : tableSizes.Split(',').Select(long.Parse).ToArray();

            var rec = new TxRec
            {
                SymbolTableIndexPointers = symbols
            };

            txLog.Create(rec);
            var rec2 = txLog.Get();

            var result =  string.Join(",", rec2.SymbolTableIndexPointers);

            Assert.That(result, Is.EqualTo(tableSizes));
        }

        [TestCase("")]
        [TestCase("40279981,40279982,4027998")]
        [TestCase("1,2,3")]
        public void Should_save_IndexPointers(string tableSizes)
        {
            var txLog = CreateTxLog();
            var symbols = string.IsNullOrEmpty(tableSizes) ?
                null : tableSizes.Split(',').Select(long.Parse).ToArray();

            var rec = new TxRec
            {
                IndexPointers = symbols
            };

            txLog.Create(rec);
            var rec2 = txLog.Get();

            var result = string.Join(",", rec2.IndexPointers);

            Assert.That(result, Is.EqualTo(tableSizes));
        }

        [TestCase("")]
        [TestCase("40279981,40279982,4027998")]
        [TestCase("1,2,3")]
        public void Should_save_LagIndexPointers(string tableSizes)
        {
            var txLog = CreateTxLog();
            var symbols = string.IsNullOrEmpty(tableSizes) ?
                null : tableSizes.Split(',').Select(long.Parse).ToArray();

            var rec = new TxRec
            {
                LagIndexPointers = symbols
            };

            txLog.Create(rec);
            var rec2 = txLog.Get();

            var result = string.Join(",", rec2.LagIndexPointers);

            Assert.That(result, Is.EqualTo(tableSizes));
        }

        private static TxLog CreateTxLog()
        {
            var stubFileF = new CompositeFileFactoryStub("_tx-0").Stub;
            var logfile = new CompositeRawFile("\\_tx",
                MetadataConstants.PIPE_BIT_HINT, stubFileF.Object,
                EFileAccess.ReadWrite, MetadataConstants.SYMBOL_PARTITION_ID,
                MetadataConstants.TX_LOG_FILE_ID, MetadataConstants.TX_LOG_FILE_ID, EDataType.Data);

            return new TxLog(logfile);
        }
    }
#endif
}