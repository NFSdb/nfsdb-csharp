using System;
using System.IO;
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.IntegrationTests.Partition
{
    [TestFixture]
    public class MultiParition
    {
        private const string FOLDER_PATH = "Multi-Partition";
        private const int TOTAL_PARITIONS = 3;
        private const int TOTAL_COUNT = (int)1E6;
        private const int GENERATE_RECORDS_COUNT_PER_PARTITION = TOTAL_COUNT / TOTAL_PARITIONS / 10;

        public class Journal
        {
            public DateTime Timestamp { get; set; }
            public string UpdatedBy { get; set; }
            public int Num1 { get; set; }
            public int Num2 { get; set; }
        }

        public class JournalGenerator : RecordGenerator<Journal>
        {
            protected override void GenerateValues(Journal item, DateTime timestamp, int i)
            {
                item.Timestamp = timestamp;
                item.Num1 = i;
                item.Num2 = i + 2;
            }
        }

        private static IJournal<Journal> OpenJournal(EFileAccess access)
        {
            string directoryPath = Path.Combine(Utils.FindJournalsPath(), FOLDER_PATH);

            return new JournalBuilder()
                .WithRecordCountHint(GENERATE_RECORDS_COUNT_PER_PARTITION)
                .WithPartitionBy(EPartitionType.Month)
                .WithLocation(directoryPath)
                .WithTimestampColumn("Timestamp")
                .WithAccess(access)
                .ToJournal<Journal>();
        }

        private IJournal<Journal> CreateJournal(EFileAccess access = EFileAccess.ReadWrite)
        {
            Utils.ClearJournal<LargeJournalPerformance.LargeJournal>(FOLDER_PATH);
            return OpenJournal(access);
        }

        public void GenerateRows()
        {
            const int totalCount = TOTAL_COUNT;
            var generator = new JournalGenerator();
            using (var journal = CreateJournal())
            {
                generator.GenerateRecords(journal, totalCount, TOTAL_PARITIONS, 1);
            }
        }

        [Test]
        public void Should_read_first_partition()
        {
            GenerateRows();
            using (var j = OpenJournal(EFileAccess.Read))
            {
                var readTx = j.OpenReadTx();
                const int shift = TOTAL_COUNT/TOTAL_PARITIONS + 1;

                for (int i = 0; i < TOTAL_COUNT; i += shift)
                {
                    var first = readTx.Items.OrderBy(t => t.Timestamp).Skip(i).First();
                    Assert.That(first.Num1, Is.EqualTo(i));
                }

            }
        }
    }
}