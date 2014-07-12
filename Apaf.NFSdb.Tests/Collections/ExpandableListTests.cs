#region copyright
/*
 * Copyright (c) 2014. APAF (Alex Pelagenko).
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