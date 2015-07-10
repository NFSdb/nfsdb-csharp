using System;
using System.Configuration;
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
    public class LargeJournalPerformance
    {
        private const string FOLDER_PATH = "Large-Journal";
        private const int TOTAL_PARITIONS = 100;
        private const int TOTAL_COUNT = (int) 100E6;
        private const int GENERATE_RECORDS_COUNT_PER_PARTITION = TOTAL_COUNT / TOTAL_PARITIONS / 2;

        public class LargeJournal
        {
            public DateTime Timestamp { get; set; }
            public string UpdatedBy { get; set; }
            public int Num1 { get; set; }
            public int Num2 { get; set; }
            public int Num3 { get; set; }
            public int Num4 { get; set; }
            public int Num5 { get; set; }
            public int Num6 { get; set; }
            public long Long1 { get; set; }
            public long Long2 { get; set; }
            public long Long3 { get; set; }
            public long Long4 { get; set; }
            public long Long5 { get; set; }
            public string Sym1 { get; set; }
            public string Sym2 { get; set; }
            public string Sym3 { get; set; }
            public string Sym4 { get; set; }
        }

        public class LargeJournalGenerator : RecordGenerator<LargeJournal>
        {
            readonly Lazy<string> _randomText = new Lazy<string>(ReadResourceText);

            private static string ReadResourceText()
            {
                using (Stream text = typeof (LargeJournal).Assembly.GetManifestResourceStream(
                    "Apaf.NFSdb.IntegrationTests.Resources.RandomText.txt"))
                {
                    if (text == null)
                    {
                        throw new ConfigurationException("Random resource text not found");
                    }
                    var sr = new StreamReader(text);
                    return sr.ReadToEnd();
                }
            }

            protected override void GenerateValues(LargeJournal item, DateTime timestamp, int i)
            {
                item.Timestamp = timestamp;
                item.Num1 = i;
                item.Num2 = i + 2;
                item.Num3 = i + 3;
                item.Num4 = i + 5;
                item.Num5 = i + 6;
                item.Long1 = i;
                item.Long2 = i*2;
                item.Long3 = i*3;
                item.Long4 = i*4;
                item.Long5 = i*5;
                item.Sym1 = _randomText.Value.Substring(
                    i % 30000, 50 + i%3);
            }
        }


        private static bool JournalExists()
        {
            string directoryPath = Path.Combine(Utils.FindJournalsPath(), FOLDER_PATH);
            return Directory.Exists(directoryPath);
        }

        private static IJournal<LargeJournal> OpenJournal(EFileAccess access)
        {
            string directoryPath = Path.Combine(Utils.FindJournalsPath(), FOLDER_PATH);

            return new JournalBuilder()
                .WithRecordCountHint(GENERATE_RECORDS_COUNT_PER_PARTITION)
                .WithPartitionBy(EPartitionType.Month)
                .WithLocation(directoryPath)
                .WithSymbolColumn("Sym1", 100000, 50, 500)
                .WithSymbolColumn("Sym2", 10000, 25, 250)
                .WithSymbolColumn("Sym3", 1000, 12, 125)
                .WithSymbolColumn("Sym4", 100, 6, 50)
                .WithSymbolColumn("Ex", 20, 20, 20)
                .WithSymbolColumn("Mode", 20, 20, 20)
                .WithTimestampColumn("Timestamp")
                .WithAccess(access)
                .ToJournal<LargeJournal>();
        }

        private IJournal<LargeJournal> CreateJournal(EFileAccess access = EFileAccess.ReadWrite)
        {
            Utils.ClearJournal<LargeJournal>(FOLDER_PATH);
            return OpenJournal(access);
        }

        public void GenerateRows()
        {
            const int totalCount = TOTAL_COUNT;
            var generator = new LargeJournalGenerator();
            using (var journal = CreateJournal())
            {
                generator.GenerateRecords(journal, totalCount, TOTAL_PARITIONS, 1);
            }
        }


        [Test]
        [Category("Performance")]
        public void Should_open_minimum_files()
        {
            if (!JournalExists())
            {
                GenerateRows();
            }

            using (var j = OpenJournal(EFileAccess.Read))
            {
                var readTx = j.OpenReadTx();
                var first = readTx.Enumerate().Take(1).ToArray();
                Console.WriteLine(j.Diagnostics.GetTotalFilesOpen());
                Console.WriteLine(j.Diagnostics.GetTotalMemoryMapped());

                Assert.That(j.Diagnostics.GetTotalFilesOpen(), Is.LessThan(27 + 18));
            }
        }
    }
}