using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Queries.Queryable.PlanItem;
using Apaf.NFSdb.Core.Tx;
using Moq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Query
{
    [TestFixture]
    public class UnionPlanItemTests
    {
        [TestCase("5,4", "4,3", ExpectedResult = "5,4,3")]
        [TestCase("5,4", "", ExpectedResult = "5,4")]
        [TestCase("", "4,3", ExpectedResult = "4,3")]
        [TestCase("", "", ExpectedResult = "")]
        [TestCase("5,4,2,0", "4,3,1,0", ExpectedResult = "5,4,3,2,1,0")]
        public string TestUnion(string first, string second)
        {
            var l1 = GetLongs(first);
            var l2 = GetLongs(second);

            var up = CreateUnionPlan(l1, l2);

            return string.Join(",", up.Execute(null, null));
        }

        private static long[] GetLongs(string first)
        {
            var l1 = first.Split(',').Where(s => !string.IsNullOrEmpty(s)).Select(long.Parse).ToArray();
            return l1;
        }

        private UnionPlanItem CreateUnionPlan(IEnumerable<long> l1,
            IEnumerable<long> l2)
        {
            var p1 = GetPlanItem(l1);
            var p2 = GetPlanItem(l2);

            return new UnionPlanItem(p1.Object, p2.Object);
        }

        private static Mock<IPlanItem> GetPlanItem(IEnumerable<long> l2)
        {
            var p2 = new Mock<IPlanItem>();
            p2.Setup(p => p.Execute(It.IsAny<IJournalCore>(), It.IsAny<IReadTransactionContext>()))
                .Returns(l2);
            p2.Setup(p => p.Timestamps).Returns(new DateRange());
            return p2;
        }
    }
}