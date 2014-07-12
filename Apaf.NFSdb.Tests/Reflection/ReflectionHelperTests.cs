﻿using Apaf.NFSdb.Core.Reflection;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Reflection
{
    [TestFixture]
    public class ReflectionHelperTests
    {
        public class TestClass
        {
            public TestClass()
            {
                Constructed = true;
            }

            public bool Constructed { get; set; }
        }

        [Test]
        public void Create_constructor_delegate_test()
        {
            var constr = ReflectionHelper.CreateConstructorDelegate(typeof (TestClass));
            var item = (TestClass) constr();
            Assert.IsTrue(item.Constructed);
        }

        [Test]
        public void Timestamp_delegate_test()
        {
            var constr = ReflectionHelper.CreateTimestampDelegate<Quote>("timestamp");
            const long timestamp = 20109236987;
            var q = new Quote {Timestamp = timestamp};

            Assert.That(constr(q), Is.EqualTo(timestamp));
        }

        [Test]
        public void Create_constructor_delegate_performance_test()
        {
            var constr = ReflectionHelper.CreateConstructorDelegate(typeof(TestClass));
            for (int i = 0; i < 1E6; i++)
            {
                var item = (TestClass) constr();
            }
        }
    }
}