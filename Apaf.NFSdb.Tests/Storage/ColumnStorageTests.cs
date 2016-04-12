using System.IO;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Moq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Storage
{
    [TestFixture]
    public class ColumnStorageTests
    {
        private Mock<ICompositeFileFactory> _fileFactory;
        private int _bitHint;
        private const string DIRECTORY_PATH = "TestJournal";

        public class TestJournal
        {
            public long LongField { get; set; }
            public int IntField { get; set; }
            public string StringField { get; set; }
            public string SymbolField { get; set; }
        }

        [TestCase(10, "intField", Result = 16)]
        [TestCase(1000, "intField", Result = 16)]
        [TestCase(250000, "intField", Result = 17)]
        [TestCase(500000, "intField", Result = 18)]
        [TestCase((int) 1E6, "intField", Result = 19)]
        [TestCase(100000000, "intField", Result = 26)]
        [TestCase(1000000000, "intField", Result = 27)]
        [TestCase(2000000000, "intField", Result = 27)]

        [TestCase(10, "longField", Result = 16)]
        [TestCase(1000, "longField", Result = 16)]
        [TestCase(250000, "longField", Result = 18)]
        [TestCase(500000, "longField", Result = 19)]
        [TestCase((int) 1E6, "longField", Result = 20)]
        [TestCase(100000000, "longField", Result = 27)]
        [TestCase(1000000000, "longField", Result = 27)]
        [TestCase(2000000000, "longField", Result = 27)]
        [TestCase(2000000000, "longField", Result = 27)]
        public int Should_calculate_bit_hint_for_fixed_columns(int recordCount, string fieldName)
        {
            // Create.
            ClearJournalFolder();
            using (var journal = new JournalBuilder()
                .WithRecordCountHint(recordCount)
                .WithLocation(DIRECTORY_PATH)
                .ToJournal<TestJournal>())
            {
                var storage = CreateStorage(journal);

                // Act.
                var col = journal.Metadata.GetColumnByPropertyName(fieldName);
                storage.GetFile(col, 1, EDataType.Data, journal.Metadata.Settings.RecordHint);

                return _bitHint;
            }
        }

        private static void ClearJournalFolder()
        {
            try
            {
                Directory.Delete(DIRECTORY_PATH, true);
            }
            catch (IOException)
            {
            }
        }

        [TestCase(1000, 50, 256, Result = 16)]
        [TestCase(1000, 500, 500, Result = 17)]
        [TestCase(1000, 5000, -1, Result = 21)]
        [TestCase(10000, 5000, -1, Result = 24)]
        [TestCase(10000, 50000, 5000, Result = 27)]
        [TestCase((int)1E6, 4, 5000, Result = 21)]
        [TestCase((int)1E6, 8, 5000, Result = 22)]
        public int Should_calculate_bit_hint_for_string_columns(int recordCount, int avSize, int maxSize)
        {
            // Create.
            ClearJournalFolder();
            const string fieldName = "stringField";
            using (var journal = new JournalBuilder()
                .WithRecordCountHint(recordCount)
                .WithStringColumn(fieldName, avSize, maxSize)
                .WithLocation(DIRECTORY_PATH)
                .ToJournal<TestJournal>())
            {

                var storage = CreateStorage(journal);

                // Act.
                var col = journal.Metadata.GetColumnByPropertyName(fieldName);
                storage.GetFile(col, 1, EDataType.Data, journal.Metadata.Settings.RecordHint);

                return _bitHint;
            }
        }

        // Datak
        [TestCase(1000, 50, 100, EDataType.Datak, Result = 11)]
        [TestCase((int)1E6, (int)1E6, (int)1E6, EDataType.Datak, Result = 11)]

        // Datar
        [TestCase((int)1E6, (int)1E6, 100, EDataType.Datar, Result = 20)]
        [TestCase((int)1E6, (int)2E6, 100, EDataType.Datar, Result = 20)]

        // Symd
        [TestCase((int)2E6, 100, 5, EDataType.Symd, Result = MetadataConstants.MIN_FILE_BIT_HINT_NON_DATA)]
        [TestCase(100, (int)1E6, 4, EDataType.Symd, Result = 21)]
        [TestCase(100, (int)1E6, 16, EDataType.Symd, Result = 23)]

        // Symrk
        [TestCase((int)1E6, (int)1E6, (int)1E6, EDataType.Symrk, Result = 11)]

        // Symrr
        [TestCase((int)2E6, 100, 16, EDataType.Symrr, Result = MetadataConstants.MIN_FILE_BIT_HINT_NON_DATA)]
        [TestCase((int)2E6, 10, (int)2E6, EDataType.Symrr, Result = MetadataConstants.MIN_FILE_BIT_HINT_NON_DATA)]
        [TestCase((int)1E6, (int)1E6, 10, EDataType.Symrr, Result = 20)]
        public int Should_calculate_bit_hint_for_symbol_columns(
            int recordCount, int distinctCount, int avgSize, EDataType dataType)
        {
            // Create.
            ClearJournalFolder();
            const string fieldName = "symbolField";
            using (var journal = new JournalBuilder()
                .WithRecordCountHint(recordCount)
                .WithSymbolColumn(fieldName, distinctCount, avgSize)
                .WithLocation(DIRECTORY_PATH)
                .ToJournal<TestJournal>())
            {
                var storage = CreateStorage(journal);

                // Act.
                var col = journal.Metadata.GetColumnByPropertyName(fieldName);
                storage.GetFile(col, 1, dataType, journal.Metadata.Settings.RecordHint);

                return _bitHint;
            }
        }

        private ColumnStorage CreateStorage(IJournal<TestJournal> journal)
        {
            _fileFactory = new Mock<ICompositeFileFactory>();
            _fileFactory.Setup(f => f.OpenFile(It.IsAny<string>(),
                It.IsAny<int>(), It.IsAny<EFileAccess>())).Callback(
                (string name, int bitHint, EFileAccess access) => _bitHint = bitHint);

            return new ColumnStorage(journal.Metadata, "temp", EFileAccess.ReadWrite, 0, _fileFactory.Object);
        }
    }
}