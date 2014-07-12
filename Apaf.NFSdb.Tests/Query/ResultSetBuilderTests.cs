using System.Linq.Expressions;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Queries.Queryable.PlanItem;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Moq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Query
{
    [TestFixture]
    public class ResultSetBuilderTests
    {
        [Test]
        public void Latest_by_id_goes_inside_union()
        {
            var union = CreateUnion();
            union.TakeLatestBy("sym");

            var plan = (UnionPlanItem) union.PlanItem;
            Assert.That(plan.Left, Is.AssignableFrom<LastestByIdPlanItem>());
            Assert.That(plan.Right, Is.AssignableFrom<LastestByIdPlanItem>());
        }

        [Test]
        public void Latest_by_id_does_not_go_to_union_of_differnt_symbol_scan()
        {
            var union = CreateUnion();
            union.TakeLatestBy("bym");

            var plan = (IntersectPlanItem)union.PlanItem;
            Assert.That(plan.Left, Is.AssignableFrom<LastestByIdPlanItem>());
            Assert.That(plan.Right, Is.AssignableFrom<UnionPlanItem>());
        }

        [Test]
        public void Latest_by_id_goes_into_intersect()
        {
            var rsb1 = CreateResultSetBuilder();
            var rsb2 = CreateResultSetBuilder();
            rsb1.IndexScan("gym", "1");
            rsb2.IndexScan("sym", "2");

            var intersect = CreateResultSetBuilder();
            intersect.Logical(rsb1, rsb2, ExpressionType.And);
            
            intersect.TakeLatestBy("sym");

            var plan = (IntersectPlanItem)intersect.PlanItem;
            Assert.That(plan.Left, Is.AssignableFrom<ColumnScanPlanItem>());
            Assert.That(plan.Right, Is.AssignableFrom<LastestByIdPlanItem>());
        }

        private ResultSetBuilder<Quote> CreateUnion()
        {
            var rsb1 = CreateResultSetBuilder();
            var rsb2 = CreateResultSetBuilder();
            rsb1.IndexScan("sym", "1");
            rsb2.IndexScan("sym", "2");

            var union = CreateResultSetBuilder();
            union.Logical(rsb1, rsb2, ExpressionType.Or);
            return union;
        }

        private ResultSetBuilder<Quote> CreateResultSetBuilder()
        {
            var journal = new Mock<IJournal<Quote>>(); 
            return new ResultSetBuilder<Quote>(journal.Object,
                new Mock<IReadTransactionContext>().Object);
        }
    }
}