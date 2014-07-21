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
using System;
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Moq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests
{
    [TestFixture]
    public class JournalMetadataTests 
    {
        [TestCase("timestamp", Result = EFieldType.Int64)]
        [TestCase("sym", Result = EFieldType.String)]
        [TestCase("bid", Result = EFieldType.Double)]
        [TestCase("bidSize", Result = EFieldType.Int32)]
        [TestCase("mode", Result = EFieldType.Int16)]
        [TestCase("pool", Result = EFieldType.Byte)]
        [TestCase("last", Result = EFieldType.Bool)]
        public EFieldType ShouldParseColumn(string colName)
        {
            var meta = CreateMetadata<FieldTypes>();
            return meta.Columns
                .First(c => c.FileName.Equals(colName, StringComparison.OrdinalIgnoreCase))
                .FieldType;
        }

        private JournalMetadata<T> CreateMetadata<T>(string[] privateFields = null)
        {
            var settings = new JournalElement();
            var journalStorage = new Mock<IColumnStorage>();
            var meta = new JournalMetadata<T>(settings);
            meta.InitializeSymbols(journalStorage.Object);
            return meta;
        }
    }
}