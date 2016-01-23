using System;
using System.IO;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Server;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Writes;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Core
{
    [TestFixture]
    public class PartitionManagerPartitionOverwriteTests
    {
        private static string _directoryPath;
        private static readonly DateTime START_DATE = new DateTime(2015, 1, 1);
        private const string FOLDER_PATH = "PartitionOverwriteTests";

        public class PocoType
        {
            public DateTime Timestamp { get; set; }
            public string Sym { get; set; }
            public int? Data { get; set; }
        }

        [TestCase(true, 40, Result = 40-31)]
        [TestCase(false, 40, Result = 40)]
        public long ShouldDiscoverPartitionNewVersionsOnNewTransactions(bool clearPartition1, int days)
        {
            using (IJournal<PocoType> journal = WriteJournal(EPartitionType.Month, TimeSpan.FromDays(1), days))
            {
                if (clearPartition1)
                {
                    var newVersion = new PartitionDate(START_DATE, 1, EPartitionType.Month).Name;
                    Directory.CreateDirectory(Path.Combine(_directoryPath, newVersion));
                }

                using (var rtx = journal.OpenReadTx())
                {
                    return rtx.All().Length ?? 0L;
                }
            }
        }

        [Test]
        public void ShouldKeepBothPartitionVersionWhenInUse()
        {
            int days = 40;
            using (IJournal<PocoType> journal = WriteJournal(EPartitionType.Month, TimeSpan.FromDays(1), days))
            {
                using (var rtx = journal.OpenReadTx())
                {
                    var newVersion = new PartitionDate(START_DATE, 1, EPartitionType.Month).Name;
                    Directory.CreateDirectory(Path.Combine(_directoryPath, newVersion));

                    using (var rtx2 = journal.OpenReadTx())
                    {
                        Assert.That(rtx2.All().Length.Value, Is.EqualTo(days - 31));
                    }
                    Assert.That(rtx.All().Length.Value, Is.EqualTo(days));
                }
            }
        }

        [Test]
        public void ShouldDeleteOutDatedPartitionVersionWhenNotInUse()
        {
            int days = 40;
            var server = new AsyncJournalServer(TimeSpan.FromSeconds(1));
            using (IJournal<PocoType> journal = WriteJournal(EPartitionType.Month, TimeSpan.FromDays(1), days, server))
            {
                using (var rtx = journal.OpenReadTx())
                {
                    var newVersion = new PartitionDate(START_DATE, 1, EPartitionType.Month).Name;
                    var newPartitionPath = Path.Combine(_directoryPath, newVersion);
                    Directory.CreateDirectory(newPartitionPath);

                    using (var rtx2 = journal.OpenReadTx())
                    {
                        var len = rtx.All().Length.Value;
                    }
                }
                
                // Act.
                server.DoEvents();

                // Verify.
                var oldVersion = new PartitionDate(START_DATE, 0, EPartitionType.Month).Name;
                var oldPartitionPath = Path.Combine(_directoryPath, oldVersion);
                Assert.That(Directory.Exists(oldPartitionPath), Is.EqualTo(false));
            }
        }

        [TestCase(true, 40, Result = 40 - 31)]
        public long ShouldUseNewPartitionOnRecreate(bool clearPartition1, int days)
        {
            var server = new AsyncJournalServer(TimeSpan.FromSeconds(1));
            using (IJournal<PocoType> journal = WriteJournal(EPartitionType.Month, TimeSpan.FromDays(1), days, server))
            {
                using (var rtx = journal.OpenReadTx())
                {
                    var newVersion = new PartitionDate(START_DATE, 1, EPartitionType.Month).Name;
                    var newPartitionPath = Path.Combine(_directoryPath, newVersion);
                    Directory.CreateDirectory(newPartitionPath);

                    using (var rtx2 = journal.OpenReadTx())
                    {
                        var len = rtx2.All().Length.Value;
                    }
                }

                // Act.
                server.DoEvents();


                // Verify.
                using (var rtx = journal.OpenReadTx())
                {
                    return rtx.All().Length.Value;
                }
            }
        }

        private static IJournal<PocoType> OpenJournal(EFileAccess access, EPartitionType type, AsyncJournalServer server)
        {
            _directoryPath = Path.Combine(Utils.FindJournalsPath(), FOLDER_PATH);
            return new JournalBuilder()
                .WithRecordCountHint((int) 1E6)
                .WithPartitionBy(type)
                .WithLocation(_directoryPath)
                .WithSymbolColumn("Sym", 20, 5, 5)
                .WithTimestampColumn("Timestamp")
                .WithAccess(access)
                .WithSerializerFactoryName(MetadataConstants.POCO_SERIALIZER_NAME)
                .WithJournalServer(server)
                .ToJournal<PocoType>();
        }

        private IJournal<PocoType> WriteJournal(EPartitionType type, TimeSpan increment, int days, AsyncJournalServer server = null)
        {
            Utils.ClearJournal<PocoType>(FOLDER_PATH);
            IJournal<PocoType> qj = OpenJournal(EFileAccess.ReadWrite, type, server);
            AppendRecords(qj, START_DATE, increment, days);
            return qj;
        }

        private static void AppendRecords(IJournal<PocoType> qj, DateTime startDate, TimeSpan increment, int days)
        {
            using (IWriter wr = qj.OpenWriteTx())
            {
                for (int i = 0; i < days; i++)
                {
                    wr.Append(new PocoType
                    {
                        Sym = "Symbol_" + i%20,
                        Timestamp = startDate.AddMilliseconds(i*increment.TotalMilliseconds)
                    });
                }
                wr.Commit();
            }
        }
    }
}