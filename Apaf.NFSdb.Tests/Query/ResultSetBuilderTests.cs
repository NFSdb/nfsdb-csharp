#region copyright
/*
 * Copyright (c) 2014. APAF http://apafltd.co.uk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion

using System.Linq.Expressions;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Queries.Queryable.PlanItem;
using Apaf.NFSdb.Core.Tx;
using Moq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Query
{
    [TestFixture]
    public class ResultSetBuilderTests
    {
        private Mock<IJournalMetadata> _metadata;

        [Test]
        public void Should_collect_or_to_single_column_filter()
        {
            var rsb1 = CreateResultSetBuilder();
            var rsb2 = CreateResultSetBuilder();
            rsb1.ColumnScan(GetColumn("sym"), "1");
            rsb2.ColumnScan(GetColumn("sym"), "2");

            var union = CreateResultSetBuilder();
            union.Logical(rsb1, rsb2, ExpressionType.Or);

            var plan = (RowScanPlanItem)union.PlanItem;
            Assert.That(plan.ToString(), Is.EqualTo("sym in (1,2)"));
        }

        [Test]
        public void Should_collect_or_to_single_column_filter_with_latest()
        {
            var rsb1 = CreateResultSetBuilder();
            var rsb2 = CreateResultSetBuilder();
            rsb1.ColumnScan(GetColumn("sym"), "1");
            rsb2.ColumnScan(GetColumn("sym"), "2");

            var union = CreateResultSetBuilder();
            union.Logical(rsb1, rsb2, ExpressionType.Or);
            union.TakeLatestBy("sym");

            var plan = (RowScanPlanItem)union.PlanItem;
            Assert.That(plan.ToString(), Is.EqualTo("Latest_By(sym in (1,2))"));
        }

        [Test]
        public void Latest_by_id_goes_inside_union()
        {
            var union = CreateUnion();
            union.TakeLatestBy("sym");

            var plan = (UnionPlanItem) union.PlanItem;
            Assert.That(plan.Left.ToString(), Is.EqualTo("Latest_By(sym in (1))"));
            Assert.That(plan.Right.ToString(), Is.EqualTo("Latest_By(sym) and bym = 2"));
        }

        [Test]
        public void Latest_by_id_does_not_go_to_union_of_different_symbol_scan()
        {
            var union = CreateUnion();
            union.TakeLatestBy("bym");

            var plan = (UnionPlanItem)union.PlanItem;
            var left = (RowScanPlanItem) plan.Left;
            var right = (RowScanPlanItem)plan.Right;
            Assert.That(left.ToString(), Is.EqualTo("Latest_By(bym) and sym = 1"));
            Assert.That(right.ToString(), Is.EqualTo("Latest_By(bym in (2))"));
        }

        [Test]
        public void Should_combine_logical_and_into_singe_row_scan()
        {
            var rsb1 = CreateResultSetBuilder();
            var rsb2 = CreateResultSetBuilder();
            rsb1.ColumnScan(GetColumn("gym"), "1");
            rsb2.ColumnScan(GetColumn("sym"), "2");

            var intersect = CreateResultSetBuilder();
            intersect.Logical(rsb1, rsb2, ExpressionType.And);

            Assert.That(intersect.PlanItem, Is.AssignableFrom<RowScanPlanItem>());
        }

        [Test]
        public void Latest_by_id_goes_into_intersect()
        {
            var rsb1 = CreateResultSetBuilder();
            var rsb2 = CreateResultSetBuilder();
            var rsb3 = CreateResultSetBuilder();
            rsb1.ColumnScan(GetColumn("gym"), "1");
            rsb2.ColumnScan(GetColumn("sym"), "2");
            rsb3.ColumnScan(GetColumn("bidSize"), 2);

            var intersect1 = CreateResultSetBuilder();
            var intersect2 = CreateResultSetBuilder();
            intersect1.Logical(rsb1, rsb2, ExpressionType.And);
            intersect2.Logical(intersect1, rsb3, ExpressionType.And);
            
            intersect2.TakeLatestBy("sym");
            var plan = (RowScanPlanItem)intersect2.PlanItem;

            Assert.That(plan.ToString(), Is.EqualTo("Latest_By(sym in (2)) and gym = 1 and bidSize = 2"));
        }

        private ResultSetBuilder CreateUnion()
        {
            var rsb1 = CreateResultSetBuilder();
            var rsb2 = CreateResultSetBuilder();
            rsb1.ColumnScan(GetColumn("sym"), "1");
            rsb2.ColumnScan(GetColumn("bym"), "2");

            var union = CreateResultSetBuilder();
            union.Logical(rsb1, rsb2, ExpressionType.Or);
            return union;
        }

        private ResultSetBuilder CreateResultSetBuilder()
        {
            var journal = new Mock<IJournalCore>();
            _metadata = new Mock<IJournalMetadata>();
            var journalStat = new Mock<IQueryStatistics>();
            journalStat.Setup(j => j.GetCardinalityByColumnValue(It.IsAny<IReadTransactionContext>(),
                It.IsAny<ColumnMetadata>(), It.IsAny<string[]>())).Returns(long.MaxValue);
            journal.Setup(j => j.Metadata).Returns(_metadata.Object);
            journal.Setup(j => j.QueryStatistics).Returns(journalStat.Object);
            _metadata.Setup(m => m.GetColumnByPropertyName(It.IsAny<string>())).Returns(
                (string name) =>
                {
                    if (name != "bidSize")
                    {
                        return ColumnMetadata.FromStringField(new ColumnSerializerMetadata(EFieldType.String, name, name),
                        10, 10, name.GetHashCode(), -1);
                    }
                    return ColumnMetadata.FromFixedField(new ColumnSerializerMetadata(EFieldType.Int32, name, name),2, -1);
                });

            return new ResultSetBuilder(journal.Object,
                new Mock<IReadTransactionContext>().Object);
        }


        private IColumnMetadata GetColumn(string sym)
        {
            return _metadata.Object.GetColumnByPropertyName(sym);
        }
    }
}