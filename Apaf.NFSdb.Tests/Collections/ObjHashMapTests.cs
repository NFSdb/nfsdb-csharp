using Apaf.NFSdb.Core.Collections;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Collections
{
    [TestFixture]
    public class ObjHashMapTests
    {
        [Test]
        public void Should_return_saved_values()
        {
            var map = new ObjIntHashMap(30);
            for (int i = 0; i < 20; i++)
            {
                map.Put("Sym_" + i, i);
            }

            for (int i = 0; i < 20; i++)
            {
                Assert.That(map.Get("Sym_"+i), Is.EqualTo(i));
            }
        }
    }
}