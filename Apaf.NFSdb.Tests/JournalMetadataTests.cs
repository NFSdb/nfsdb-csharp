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
using Apaf.NFSdb.Core.Exceptions;
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


        [TestCase("timestamp,sym,bid,bidSize,mode,pool,last", Result = "Timestamp,Sym,Bid,BidSize,Mode,Pool,Last,_nulls")]
        [TestCase("sym,bid,bidSize,mode,pool,last,timestamp", Result = "Sym,Bid,BidSize,Mode,Pool,Last,Timestamp,_nulls")]
        [TestCase("bidSize,pool,sym,bid,mode,last,timestamp", Result = "BidSize,Pool,Sym,Bid,Mode,Last,Timestamp,_nulls")]
        [TestCase("bidSize,pool,sym,bid,mode,last", ExpectedException = typeof(NFSdbConfigurationException))]
        [TestCase("timestamp,timestamp,sym,bid,bidSize,mode,pool", ExpectedException = typeof(NFSdbConfigurationException))]
        public string ShouldCheckColumsMatch(string colNames)
        {
            var noConfMeta = CreateMetadata<FieldTypes>();

            var conf = new JournalElement
            {
                Columns = colNames.Split(new[] {','})
                    .Select(n =>
                    {
                        var column = noConfMeta.GetColumnByPropertyName(n);
                        return column.FieldType == EFieldType.String
                            ? new StringElement {Name = n}
                            : new ColumnElement {Name = n, ColumnType = column.FieldType};
                    }).ToList(),
                    FromDisk = true
            };

            var meta = CreateMetadata<FieldTypes>(conf);
            return string.Join(",", meta.Columns.Select(c => c.PropertyName));
        }

        private JournalMetadata CreateMetadata<T>(JournalElement conf = null, string[] privateFields = null)
        {
            var settings = conf ?? new JournalElement();
            var journalStorage = new Mock<IColumnStorage>();
            var meta = JournalBuilder.CreateNewJournalMetadata(settings, typeof(T));
            meta.InitializeSymbols(journalStorage.Object);
            return meta;
        }
    }
}