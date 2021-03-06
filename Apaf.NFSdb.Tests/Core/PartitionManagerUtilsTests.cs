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
using System;
using System.Globalization;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Storage;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Core
{
    [TestFixture]
    public class PartitionManagerUtilsTests
    {
        [TestCase("2013", EPartitionType.Year, ExpectedResult = "2013-01-01 00:00:00")]
        [TestCase("2013.123", EPartitionType.Year, ExpectedResult = "2013-01-01 00:00:00")]
        [TestCase("2013.1-2", EPartitionType.Year, ExpectedException = typeof(InvalidOperationException))]
        [TestCase("2013-03", EPartitionType.Month, ExpectedResult = "2013-03-01 00:00:00")]
        [TestCase("2013-03-05", EPartitionType.Day, ExpectedResult = "2013-03-05 00:00:00")]
        [TestCase("2013-03-05.1", EPartitionType.Day, ExpectedResult = "2013-03-05 00:00:00")]
        [TestCase("2013-03-05.a", EPartitionType.Day, ExpectedException = typeof(InvalidOperationException))]
        [TestCase("default", EPartitionType.None, ExpectedResult = "0001-01-01 00:00:00")]
        [TestCase("default.1", EPartitionType.None, ExpectedResult = "0001-01-01 00:00:00")]
        [TestCase("default.0", EPartitionType.None, ExpectedResult = "0001-01-01 00:00:00")]
        [TestCase("2013-03-05", EPartitionType.Year, ExpectedException = typeof(InvalidOperationException))]
        [TestCase("2013", EPartitionType.None, ExpectedException = typeof(InvalidOperationException))]
        public string ShouldParseDirectoryNames(string directoryName,
            EPartitionType partitionType)
        {
            var date = PartitionManagerUtils.ParseDateFromDirName(directoryName, partitionType);
            return date.Value.Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        }
    }
}