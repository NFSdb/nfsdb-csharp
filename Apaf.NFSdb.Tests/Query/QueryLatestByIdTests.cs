﻿#region copyright
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

using System.Linq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Query
{
    [TestFixture]
    public class QueryLatestByIdTests
    {
        [Test]
        public void Should_return_correct_latest()
        {
            var latestIds = ExecuteLatestBySymUtil.ExecuteLambda(items => items, 1);
            Assert.That(latestIds, Is.EqualTo("299,298,297,296,295,294,293,292,291,290,289,288,287,286,285,284,283,282,281,280"));
        }

        [Test]
        public void Should_return_correct_latest_with_nulls()
        {
            var latestIds = ExecuteLatestBySymUtil.ExecuteLambda(items => items, 1, true);
            Assert.That(latestIds, Is.EqualTo("299,298,297,296,295,294,293,292,291,290,289,288,287,286,285,284,283,282,281,280"));
        }

        [Test]
        public void Should_return_correct_latest_limted_timestamps()
        {
            var latestIds = ExecuteLatestBySymUtil.ExecuteLambda(items =>
                from q in items
                where q.Timestamp < 100
                select q, 1);

            Assert.That(latestIds, Is.EqualTo("99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80"));
        }

        [Test]
        public void Should_return_correct_latest_limted_timestamps2()
        {
            var latestIds = ExecuteLatestBySymUtil.ExecuteLambda(items =>
                from q in items
                where q.Timestamp >=290
                select q, 1);

            Assert.That(latestIds, Is.EqualTo("299,298,297,296,295,294,293,292,291,290"));
        }

        [Test]
        public void Should_return_correct_latest_limted_timestamps3()
        {
            var latestIds = ExecuteLatestBySymUtil.ExecuteLambda(items =>
                from q in items
                where q.Timestamp >= 90 && q.Timestamp < 100
                select q, 1);

            Assert.That(latestIds, Is.EqualTo("99,98,97,96,95,94,93,92,91,90"));
        }

        [TestCase("Symbol_0", ExpectedResult = "280")]
        [TestCase("Symbol_1", ExpectedResult = "281")]
        [TestCase("Symbol_19", ExpectedResult = "299")]
        [TestCase("Symbol_20", ExpectedResult = "")]
        public string Should_return_correct_latest_by_single_key(string sym)
        {
            var latestIds = ExecuteLatestBySymUtil.ExecuteLambda(items =>
                from q in items
                where q.Sym == sym
                select q, 1);
            return latestIds;
        }

        [TestCase("Symbol_0,Symbol_19", ExpectedResult = "299,280")]
        [TestCase("Symbol_19,Symbol_0", ExpectedResult = "299,280")]
        [TestCase("Symbol_19,Symbol_20,Symbol_0", ExpectedResult = "299,280")]
        [TestCase("Symbol_19,Symbol_15,Symbol_0", ExpectedResult = "299,295,280")]
        [TestCase("Symbol_20", ExpectedResult = "")]
        public string Should_return_correct_latest_by_list_of_keys(string sym)
        {
            var symbols = sym.Split(',');
            var latestIds = ExecuteLatestBySymUtil.ExecuteLambda(items =>
                from q in items
                where symbols.Contains(q.Sym)
                select q, 1);
            return latestIds;
        }

        [TestCase("Ex_0", ExpectedResult = "280")]
        [TestCase("Ex_11", ExpectedResult = "291")]
        [TestCase("Ex_111", ExpectedResult = "")]
        public string Should_return_correct_latest_filtered_by_another_symbol(string ex)
        {
            var latestIds = ExecuteLatestBySymUtil.ExecuteLambda(items =>
                from q in items
                where q.Ex == ex
                select q, 1);
            return latestIds;
        }
    }
}