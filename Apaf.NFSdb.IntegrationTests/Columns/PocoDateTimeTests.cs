using System;
using System.Globalization;
using System.IO;
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Writes;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.IntegrationTests.Columns
{
    [TestFixture]
    public class PocoDateTimeTests
    {
        private static long _timeIncrement;
        private const string FOLDER_PATH = "DateTime";

        public class File
        {
            public string Path { get; set; }
            public DateTime? Created { get; set; }
            public DateTime Modified { get; set; }
        }

        private IJournal<File> CreateJournal(DateTimeMode mode, EFileAccess access = EFileAccess.ReadWrite)
        {
            Utils.ClearJournal<File>(FOLDER_PATH);
            return OpenJournal(access, mode);
        }

        private static IJournal<File> OpenJournal(EFileAccess access, DateTimeMode mode)
        {
            string directoryPath = Path.Combine(Utils.FindJournalsPath(), FOLDER_PATH);
            var jb = new JournalBuilder()
                .WithRecordCountHint(TestUtils.GENERATE_RECORDS_COUNT)
                .WithPartitionBy(EPartitionType.Month)
                .WithLocation(directoryPath)
                .WithSymbolColumn("Path", 10000, 20, 20)
                .WithTimestampColumn("Modified");

            if (mode == DateTimeMode.EpochMilliseconds)
            {
                jb = jb.WithEpochDateTimeColumn("Modified")
                    .WithEpochDateTimeColumn("Created");
            }
            return jb.WithAccess(access).ToJournal<File>();
        }

        public void GenerateFile(File item, long timestampIncr, int i)
        {
            _timeIncrement = timestampIncr;
            item.Path = i.ToString(CultureInfo.InvariantCulture);
            item.Modified = TestUtils.START.AddMilliseconds(i*timestampIncr);
            item.Created = i%2 == 0
                ? TestUtils.START.AddMilliseconds(i * timestampIncr * 2)
                : (DateTime?) null;
        }

        [Test]
        public void ShouldReadWriteEpochMillisecondsDatTime()
        {
            using (IJournal<File> journal = CreateJournal(DateTimeMode.EpochMilliseconds))
            {
                TestUtils.GenerateRecords(journal, 1000, 2, GenerateFile);
                IQuery<File> r2 = journal.OpenReadTx();

                int i = 0;
                foreach (File file in r2.All())
                {
                    Assert.That(file.Path, Is.EqualTo(i.ToString(CultureInfo.InvariantCulture)));
                    DateTime expectedModifedDate =
                        TestUtils.START.AddMilliseconds(i * _timeIncrement);

                    Assert.That(DateUtils.DateTimeToUnixTimeStamp(file.Modified)
                        , Is.EqualTo(DateUtils.DateTimeToUnixTimeStamp(expectedModifedDate)));

                    DateTime? expectedCreatedDate = i%2 == 0
                        ? TestUtils.START.AddMilliseconds(i * _timeIncrement * 2)
                        : (DateTime?) null;

                    Assert.That(file.Created, Is.EqualTo(expectedCreatedDate));
                    i++;
                }
            }
        }

        [Test]
        public void ShouldReadWriteNetDateTimes()
        {
            using (IJournal<File> journal = CreateJournal(DateTimeMode.DotNetDateTime))
            {
                TestUtils.GenerateRecords(journal, 1000, 2, GenerateFile);
                IQuery<File> r2 = journal.OpenReadTx();

                int i = 0;
                foreach (File file in r2.All())
                {
                    Assert.That(file.Path, Is.EqualTo(i.ToString(CultureInfo.InvariantCulture)));
                    Assert.That(file.Modified,
                        Is.EqualTo(TestUtils.START.AddMilliseconds(i * _timeIncrement)));

                    Assert.That(file.Created, Is.EqualTo(
                        i%2 == 0
                            ? TestUtils.START.AddMilliseconds(i * _timeIncrement * 2)
                            : (DateTime?) null));
                    i++;
                }
            }
        }

        [Test]
        public void ShouldSupportSearchByDateTimeTimestamp()
        {
            using (IJournal<File> journal = CreateJournal(DateTimeMode.DotNetDateTime))
            {
                const int count = 1000;
                TestUtils.GenerateRecords(journal, count, 2, GenerateFile);
                IQuery<File> r2 = journal.OpenReadTx();

                for(int i = 0; i < count; i++)
                {
                    var recordTimestamp = TestUtils.START.AddMilliseconds(i * _timeIncrement);
                    var file = r2.Items.Single(j => j.Modified == recordTimestamp);

                    Assert.That(file.Path, Is.EqualTo(i.ToString(CultureInfo.InvariantCulture)));
                    Assert.That(file.Created, Is.EqualTo(
                        i % 2 == 0
                            ? TestUtils.START.AddMilliseconds(i * _timeIncrement * 2)
                            : (DateTime?)null));
                    i++;
                }
            }
        }
    }
}