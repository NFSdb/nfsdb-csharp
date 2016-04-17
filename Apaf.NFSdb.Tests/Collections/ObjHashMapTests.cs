using Apaf.NFSdb.Core.Collections;
using Apaf.NFSdb.Core.Column;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Collections
{
    [TestFixture]
    public class ObjHashMapTests
    {
        [Test]
        public void Should_save_null()
        {
            var map = new ObjIntHashMap(30);
            map.Put(null, 1);
            Assert.That(map.Get(null), Is.EqualTo(1));
        }

        [Test]
        public void Should_return_saved_values()
        {
            var map = new ObjIntHashMap(30);
            for (int i = 0; i < 20; i++)
            {
                map.Put("Sym_" + i, i);
            }

            for (int i = 0; i < 30; i++)
            {
                Assert.That(map.Get("Sym_" + i), Is.EqualTo(i < 20 ? i : MetadataConstants.SYMBOL_NOT_FOUND_VALUE));
            }
        }

        [Test]
        public void Should_clear_prev_values()
        {
            var map = new ObjIntHashMap(30);
            for (int i = 0; i < 20; i++)
            {
                map.Put("Sym_" + i, i);
            }
            map.Clear();

            for (int i = 0; i < 20; i++)
            {
                Assert.That(map.Get("Sym_" + i), Is.EqualTo(MetadataConstants.SYMBOL_NOT_FOUND_VALUE));
            }
        }

        [Test]
        [Ignore]
        public void Should_lookup_values()
        {
            // TODO
            var map = new ObjIntHashMap(1024);
            for (int i = 0; i < 2048; i++)
            {
                map.Put("Sym_" + i, i);
            }

            for (int i = 0; i < 2048; i++)
            {
                var expected = "Sym_" + i;
                string actual;
                map.LookupValue(i, out actual);
                Assert.That(actual, Is.EqualTo(expected), "iteration " + i);
            }
        }
    }
}