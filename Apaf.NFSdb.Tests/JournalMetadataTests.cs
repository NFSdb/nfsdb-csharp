using System;
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Moq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests
{
    [TestFixture]
    public class JournalMetadataTests 
    {
        [TestCase("timestamp", Result = EFieldType.Int64)]
        [TestCase("sym", Result = EFieldType.String)]
        [TestCase("bid", Result = EFieldType.Double)]
        [TestCase("bidSize", Result = EFieldType.Int32)]
        [TestCase("mode", Result = EFieldType.Int16)]
        [TestCase("pool", Result = EFieldType.Byte)]
        [TestCase("last", Result = EFieldType.Bool)]
        public EFieldType ShouldParseColumn(string colName)
        {
            var meta = CreateMetadata<FieldTypes>();
            return meta.Columns
                .First(c => c.FieldName.Equals(colName, StringComparison.OrdinalIgnoreCase))
                .FieldType;
        }

        private JournalMetadata<T> CreateMetadata<T>(string[] privateFields = null)
        {
            var settings = new JournalElement();
            var journalStorage = new Mock<IColumnStorage>();
            var meta = new JournalMetadata<T>(settings);
            meta.InitializeSymbols(journalStorage.Object);
            return meta;
        }
    }
}