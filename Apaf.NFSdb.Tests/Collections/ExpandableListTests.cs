using Apaf.NFSdb.Core.Collections;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Collections
{
    [TestFixture]
    public class ExpandableListTests
    {
        [Test]
        public void Should_create_item_on_get()
        {
            var el = new ExpandableList<ExpandableList<long>>(
                () => new ExpandableList<long>());

            el[3][1] = 12;

            Assert.That(el[3][1], Is.EqualTo(12));
        }

        [Test]
        public void Should_increment_correctly()
        {
            var el = new ExpandableList<ExpandableList<long>>(
                () => new ExpandableList<long>());

            el[3][1] = 12;
            el[3][1] += 8;

            Assert.That(el[3][1], Is.EqualTo(20));
        }

        [Test]
        public void Should_return_default_on_get_not_existing()
        {
            var el = new ExpandableList<long>();

            var actual = el[2];
            Assert.That(actual, Is.EqualTo(0));
        }

        [Test]
        public void Should_return_default_created_by_delegate_on_get_not_existing()
        {
            var el = new ExpandableList<long>(() => 345L);

            var actual = el[2];
            Assert.That(actual, Is.EqualTo(345L));
        }
    }
}