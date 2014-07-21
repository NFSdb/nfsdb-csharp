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
using Apaf.NFSdb.Core.Reflection;
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