using System.Globalization;
using System.IO;
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.IntegrationTests.Columns
{
    [TestFixture]
    public class PocoBinaryTests
    {
        private const string FolderPath = "File";

        public class File
        {
            public string Path { get; set; }
            public byte[] Content { get; set; }
            public long Timestamp { get; set; }
        }

        private IJournal<File> CreateJournal(EFileAccess access = EFileAccess.ReadWrite)
        {
            Utils.ClearJournal<File>(FolderPath);
            return OpenJournal(access);
        }

        private static IJournal<File> OpenJournal(EFileAccess access)
        {
            string directoryPath = Path.Combine(Utils.FindJournalsPath(), FolderPath);
            return new JournalBuilder()
                .WithRecordCountHint(TestUtils.GENERATE_RECORDS_COUNT)
                .WithPartitionBy(EPartitionType.Month)
                .WithLocation(directoryPath)
                .WithSymbolColumn("Path", 10000, 20, 20)
                .WithTimestampColumn("Timestamp")
                .WithAccess(access)
                .ToJournal<File>();
        }

        [Test]
        public void ShouldReadWriteFiles()
        {
            using (var journal = CreateJournal())
            {
                TestUtils.GenerateRecords(journal, 1000, 2, GenerateFile);
                var r2 = journal.OpenReadTx();

                int i = 0;
                foreach (var file in r2.All())
                {
                    Assert.That(file.Path, Is.EqualTo(i.ToString(CultureInfo.InvariantCulture)));
                    Assert.That(file.Content.Length, Is.EqualTo(i));
                    for (int j = 0; j < file.Content.Length; j++)
                    {
                        Assert.AreEqual(file.Content[j], (byte)j);
                    }
                    i++;
                }
            }
        }

        [Test]
        public void ShouldReadBySymbol()
        {
            int count = 1000;
            using (var journal = CreateJournal())
            {
                TestUtils.GenerateRecords(journal, count, 2, GenerateFile);
                var r2 = journal.OpenReadTx();

                for(int i = 0; i < count; i++)
                {
                    var file = r2.Items.Single(j => j.Path == i.ToString(CultureInfo.InvariantCulture));
                    for (int j = 0; j < file.Content.Length; j++)
                    {
                        Assert.AreEqual(file.Content[j], (byte)j);
                    }
                    i++;
                }
            }
        }

        public static void GenerateFile(File item, long timestampIncr, int i)
        {
            item.Path = i.ToString();
            item.Timestamp = TestUtils.START_TIMESTAMP + i * timestampIncr;
            item.Content = new byte[i];
            for (int j = 0; j < i; j++)
            {
                item.Content[j] = (byte) j;
            }
        }
    }
}