using System;
using System.Collections;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Writes;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Columns
{
    [TestFixture]
    public class DateTimeTests
    {
        [Test]
        public void Greater_Is_Not_Affected_By_Date_Kind()
        {
            var date1 = new DateTime(2014, 6, 1, 0, 0, 0, 1, DateTimeKind.Local);
            var date2 = new DateTime(2014, 6, 1, 0, 0, 0, 0, DateTimeKind.Utc);

            Assert.IsTrue(date1 > date2);
            Assert.IsTrue(date1.ToUniversalTime() < date2.ToUniversalTime());
        }

        [TestCaseSource("AllKindDateTimes")]
        public unsafe void ShouldRemoveDateKind(DateTime value)
        {
            long toLong = DateUtils.ToUnspecifiedDateTicks(value);
            DateTime converted = ((DateTime*)&toLong)[0];

            Assert.That(converted.Ticks, Is.EqualTo(value.Ticks));
            Assert.That(converted.Kind, Is.EqualTo(DateTimeKind.Unspecified));
        }

        public IEnumerable<TestCaseData> AllKindDateTimes()
        {
            yield return new TestCaseData(new DateTime(2014, 6, 1, 0, 0, 0, 0, DateTimeKind.Utc));
            yield return new TestCaseData(new DateTime(2014, 6, 1, 0, 0, 0, 1, DateTimeKind.Local));
            yield return new TestCaseData(new DateTime(2014, 6, 1, 0, 2, 0, 1, DateTimeKind.Unspecified));
            yield return new TestCaseData(new DateTime(DateTime.MinValue.Ticks, DateTimeKind.Utc));
            yield return new TestCaseData(new DateTime(DateTime.MaxValue.Ticks, DateTimeKind.Unspecified));
            yield return new TestCaseData(new DateTime(DateTime.MaxValue.Ticks, DateTimeKind.Local));
        }
    }
}