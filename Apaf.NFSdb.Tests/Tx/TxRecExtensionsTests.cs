using System;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Core.Writes;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Tx
{
    [TestFixture]
    public class TxRecExtensionsTests
    {
        [TestCase("01-09-2009", 1, ExpectedResult = true)]
        [TestCase("24-01-2012", 1, ExpectedResult = false)]
        [TestCase("23-01-2012", 2, ExpectedResult = true)]
        public bool Should_use_last_partition_timestamp(string partStart, int partitionID)
        {
            long timestamp = DateUtils.DateTimeToUnixTimeStamp(new DateTime(2012, 01, 23));
            var txRec = CreateTxRec(timestamp, 0);

            var partStartDate = DateTime.ParseExact(partStart, "dd-MM-yyyy", null);

            // ReSharper disable once InvokeAsExtensionMethod
            // Act.
            var isCommited = TxRecExtensions.IsCommited(txRec, partStartDate, partitionID);

            return isCommited;
        }

        [TestCase("01-09-2009", 1, ExpectedResult = true)]
        [TestCase("24-01-2012", 1, ExpectedResult = true)]
        [TestCase("23-01-2012", 2, ExpectedResult = false)]
        public bool Should_use_partition_id_when_timestamp_is_0(string partStart, int partitionID)
        {
            var txRec = CreateTxRec(timestamp: 0, journalMaxRec: 1001);
            var partStartDate = DateTime.ParseExact(partStart, "dd-MM-yyyy", null);

            // ReSharper disable once InvokeAsExtensionMethod
            // Act.
            var isCommited = TxRecExtensions.IsCommited(txRec, partStartDate, partitionID);

            return isCommited;
        }
        
        private TxRec CreateTxRec(long timestamp, int journalMaxRec)
        {
            return new TxRec
            {
                LastPartitionTimestamp = timestamp,
                JournalMaxRowID = journalMaxRec
            };
        }
    }
}