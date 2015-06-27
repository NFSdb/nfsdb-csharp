using System;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Unsafe
{
    public class UnsafeTests
    {
        public class MyClass
        {
            public long Field;
        }

        [Test]
        public unsafe void MutatingString()
        {
            var str = new string(new[] { 'o', 'n', 'e' });
            var cl = new MyClass();
            fixed (char* ptr = str)
            {
                char t = ptr[0];
                ptr[0] = ptr[1];
                ptr[1] = t;
            }
            Console.WriteLine(str);
            Assert.That(str, Is.EqualTo("noe"));
        }
    }
}