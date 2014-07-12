using Apaf.NFSdb.Core.Queries;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Query
{
    [TestFixture]
    public class RowIDUtilTests
    {
        [TestCase(1, int.MaxValue *3L)]
        [TestCase(2048, int.MaxValue * 10L)]
        [TestCase(0, 1023)]
        [TestCase(0, 1)]
        public void ShouldCreateRowIdsWithCorrect(int partitionId, long rowID)
        {
            var globalRowID = RowIDUtil.ToRowID(partitionId, rowID);
            var actualPartitionID = RowIDUtil.ToPartitionIndex(globalRowID);
            var actualRowID = RowIDUtil.ToLocalRowID(globalRowID);

            Assert.That(actualPartitionID, Is.EqualTo(partitionId));
            Assert.That(actualRowID, Is.EqualTo(rowID));
        }
    }
}