using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Queries;
using Moq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Query
{
    [TestFixture]
    public class ColumnValueBinarySearchTests
    {
        [TestCase(990, 50, 100, ExpectedResult = 99)]
        [TestCase(990, 0, 100, ExpectedResult = 99)]
        [TestCase(0, 0, 100, ExpectedResult = 0)]
        [TestCase(0, 0, 10, ExpectedResult = 0)]
        [TestCase(50, 0, 100, ExpectedResult = 5)]
        [TestCase(49, 0, 100, ExpectedResult = ~5)]
        [TestCase(1, 0, 100, ExpectedResult = ~1)]
        [TestCase(-1, 0, 100, ExpectedResult = ~0)]
        [TestCase(long.MaxValue, 0, 100, ExpectedResult = ~100)]
        public long Boundary_test(long value, long index, long count)
        {
            var longArray = Enumerable.Range(0, 100)
                .Select(i => (long)i*10).ToArray();

            var col = MockFixedCol(longArray);
            return ColumnValueBinarySearch.LongBinarySerach(
                col, value, index, count);
        }

        private IFixedWidthColumn MockFixedCol(long[] longArray)
        {
            var fc = new Mock<IFixedWidthColumn>();
            fc.Setup(f => f.GetInt64(It.IsAny<long>()))
                .Returns((long i) => longArray[(int) i]);
            return fc.Object;
        }
    }
}