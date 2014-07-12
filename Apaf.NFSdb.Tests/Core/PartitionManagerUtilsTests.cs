using System;
using System.Globalization;
using Apaf.NFSdb.Core;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Core
{
    [TestFixture]
    public class PartitionManagerUtilsTests
    {
        [TestCase("2013", EPartitionType.Year, ExpectedResult = "2013-01-01 00:00:00")]
        [TestCase("2013-03", EPartitionType.Month, ExpectedResult = "2013-03-01 00:00:00")]
        [TestCase("2013-03-05", EPartitionType.Day, ExpectedResult = "2013-03-05 00:00:00")]
        [TestCase("default", EPartitionType.None, ExpectedResult = "0001-01-01 00:00:00")]
        [TestCase("2013-03-05", EPartitionType.Year, ExpectedException = typeof(InvalidOperationException))]
        [TestCase("2013", EPartitionType.None, ExpectedException = typeof(InvalidOperationException))]
        public string ShouldParseDirectoryNames(string directoryName,
            EPartitionType partitionType)
        {
            var date = PartitionManagerUtils.ParseDateFromDirName(directoryName, partitionType);
            return date.Value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        }
    }
}