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
using Apaf.NFSdb.Core.Queries;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Query
{
    [TestFixture]
    public class RowIDUtilTests
    {
        [TestCase(1, int.MaxValue *3L)]
        [TestCase(2048, int.MaxValue * 10L)]
        [TestCase(0, 1023)]
        [TestCase(0, 1)]
        public void ShouldCreateRowIdsWithCorrect(int partitionId, long rowID)
        {
            var globalRowID = RowIDUtil.ToRowID(partitionId, rowID);
            var actualPartitionID = RowIDUtil.ToPartitionIndex(globalRowID);
            var actualRowID = RowIDUtil.ToLocalRowID(globalRowID);

            Assert.That(actualPartitionID, Is.EqualTo(partitionId));
            Assert.That(actualRowID, Is.EqualTo(rowID));
        }
    }
}