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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Storage.Serializer;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Apaf.NFSdb.Tests.Tx;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Serializer
{
    [TestFixture]
    public class ThriftObjectSerializerTests
    {
        [TestCase("timestamp", ExpectedResult = true)]
        [TestCase("ask", ExpectedResult = true)]
        [TestCase("bid", ExpectedResult = true)]
        [TestCase("askSize", ExpectedResult = false)]
        [TestCase("bidSize", ExpectedResult = true)]
        [TestCase("sym", ExpectedResult = false)]
        [TestCase("ex", ExpectedResult = true)]
        [TestCase("mode", ExpectedResult = true)]
        public bool ShouldProcessBitset(string propertyName)
        {
            var s = new Quote
            {
                Timestamp = 12345,
                Ask = 2.3,
                Bid = 2.4,
                BidSize = 0,
                AskSize = 34,
                Ex = "qwerty",
                Mode = ""
            };
            s.__isset.askSize = false;
            s.__isset.ex = true;
            var rdr = CreateReader(s);
            
            var deser = (Quote)rdr.Read(0, null);
            return (bool) typeof (Quote.Isset).GetField(propertyName)
                .GetValue(deser.__isset);
        }

        [TestCase("mode", ExpectedResult = false)]
        [TestCase("sym", ExpectedResult = true)]
        public bool ShouldProcessNullStrings(string propertyName)
        {
            var s = new Quote
            {
                Sym = null,
                Mode = null
            };
            s.__isset.mode = false;
            s.__isset.sym = true;
            var rdr = CreateReader(s);

            var deser = (Quote)rdr.Read(0, null);
            return (bool)typeof(Quote.Isset).GetField(propertyName)
                .GetValue(deser.__isset);
        }


        [Test]
        public void ShouldReadFirstInt64()
        {
            var rdr = CreateReader(new Quote {Timestamp = 12345});
            var deser = (Quote)rdr.Read(0, null);
            Assert.That(deser.Timestamp, Is.EqualTo(12345));
        }

        [Test]
        public void ShouldReadDouble()
        {
            var rdr = CreateReader(new Quote { Bid = 0.12345 });
            var deser = (Quote)rdr.Read(0, null);
            Assert.That(deser.Bid, Is.EqualTo(0.12345));
        }

        [Test]
        public void ShouldReadInt32()
        {
            var q = new Quote {BidSize = int.MaxValue};
            var rdr = CreateReader(q);
            var deser = (Quote)rdr.Read(0, null);
            Assert.That(deser.BidSize, Is.EqualTo(int.MaxValue));
        }

        [Test]
        public void ShouldReadIntString()
        {
            const string blabla = "Blabla";
            var rdr = CreateReader(new Quote { Ex = blabla });
            var deser = (Quote)rdr.Read(0, null);
            Assert.That(deser.Ex, Is.EqualTo(blabla));
        }

        [Test]
        public void ShouldReadKeepNullString()
        {
            const string blabla = "Blabla";
            var rdr = CreateReader(new Quote { Ex = blabla });
            var deser = (Quote)rdr.Read(0, null);
            Assert.That(deser.Mode, Is.Null);
        }

        [Test]
        public void ShouldPreserveUnsetValues()
        {
            const string blabla = "Blabla";
            var rdr = CreateReader(new Quote { Ex = blabla });
            var deser = (Quote)rdr.Read(0, null);
            Assert.That(deser.__isset.timestamp, Is.False);
        }

        [TestCase("Timestamp", ExpectedResult = 12345)]
        [TestCase("Ask", ExpectedResult = 2.3)]
        [TestCase("Bid", ExpectedResult = 2.4)]
        [TestCase("BidSize", ExpectedResult = 0)]
        [TestCase("AskSize", ExpectedResult = 34)]
        [TestCase("Ex", ExpectedResult = "qwerty")]
        [TestCase("Mode", ExpectedResult = "")]
        [TestCase("Sym", ExpectedResult = null)]
        public object ShouldWriteColumnValues(string propertyName)
        {
            var columns = GetQuoteColumns(new Quote());
            var s = new Quote
            {
                Timestamp = 12345,
                Ask = 2.3,
                Bid = 2.4,
                BidSize = 0,
                AskSize = 34,
                Ex = "qwerty",
                Mode = ""
            };
            var serializer = CreateWriter(columns);
            
            // Act.
            serializer.Write(s, 0, TestTxLog.TestContext());
            
            // Verify.
            var resultCol = (IColumnStub)columns.First(c => c.PropertyName == propertyName);
            return resultCol.Value;
        }

        [TestCase("Timestamp", ExpectedResult = false)]
        [TestCase("Ask", ExpectedResult = true)]
        [TestCase("Bid", ExpectedResult = false)]
        [TestCase("BidSize", ExpectedResult = false)]
        [TestCase("AskSize", ExpectedResult = true)]
        [TestCase("Ex", ExpectedResult = false)]
        [TestCase("Mode", ExpectedResult = false)]
        [TestCase("Sym", ExpectedResult = true)]
        public bool ShouldWriteBitmapValues(string propertyName)
        {
            var s = new Quote
            {
                Timestamp = 12345,
                Bid = 2.4,
                BidSize = 0,
                Ex = "qwerty",
                Mode = ""
            };
            var columns = GetQuoteColumns(s);
            var serializer = CreateWriter(columns);

            // Act.
            serializer.Write(s, 0, TestTxLog.TestContext());

            // Verify.
            // Probed column.
            var searchCol = columns.First(c => c.PropertyName == propertyName);
            // Bitset.
            var resultCol = (QuoteBitsetColumnStub)columns.First(c => c.FieldType == EFieldType.BitSet);
            // Non-bitset.
            var columnList = new List<IColumn>(columns.Where(c => c.FieldType != EFieldType.BitSet));
            return resultCol.SetColumnIndecies.Contains(columnList.IndexOf(searchCol));
        }

#if RELEASE
        [Test]
        [Category("Performance")]
        public void WriteSpeed()
        {
            var columns = GetQuoteColumns(new Quote());
            var s = new Quote
            {
                Timestamp = 12345,
                Bid = 2.4,
                Ask = double.MaxValue,
                BidSize = 0,
                Ex = "qwerty",
                Mode = ""
            };
            var serializer = CreateWriter(columns);

            // Act.
            var rdrCntx = TestTxLog.TestContext();
            var timer = new Stopwatch();
            timer.Start();

            for (int i = 0; i < 1E7; i++)
            {
                serializer.Write(s, 0, rdrCntx);
            }
            timer.Stop();
            Console.WriteLine(timer.Elapsed);
            Assert.That(timer.Elapsed.Seconds, Is.LessThanOrEqualTo(1));
        }

        [Test]
        [Category("Performance")]
        public void ReadSpeed()
        {
            var s = new Quote
            {
                Timestamp = 12345,
                Ask = 2.3,
                Bid = double.MaxValue,
                BidSize = 0,
                AskSize = 34,
                Ex = "qwerty",
                Mode = ""
            };
            var rdr = CreateReader(s);
            var rdrCntx = new ReadContext();
            var timer = new Stopwatch();
            timer.Start();
            
            for (int i = 0; i < 1E7; i++)
            {
                rdr.Read(0, rdrCntx);
            }
            timer.Stop();
            Console.WriteLine(timer.Elapsed);
            Assert.That(timer.Elapsed.Seconds, Is.LessThanOrEqualTo(1));
        }
#endif

        private ObjectSerializer CreateReader(Quote t)
        {
            var serializerFactory = new ThriftSerializerFactory();
            serializerFactory.Initialize(typeof(Quote));

            var columns = GetQuoteColumns(t);
            return (ObjectSerializer)serializerFactory.CreateFieldSerializer(columns);
        }

        private ObjectSerializer CreateWriter(IColumn[] columns)
        {
            var serializerFactory = new ThriftSerializerFactory();
            serializerFactory.Initialize(typeof(Quote));

            return (ObjectSerializer)serializerFactory.CreateFieldSerializer(columns);
        }

        private static IColumn[] GetQuoteColumns(Quote t)
        {
            var columns = new[]
            {
                ColumnsStub.CreateColumn(t.Timestamp, EFieldType.Int64, 1, "Timestamp"),
                ColumnsStub.CreateColumn(t.Sym, EFieldType.String, 2, "Sym"),
                ColumnsStub.CreateColumn(t.Bid, EFieldType.Double, 3, "Bid"),
                ColumnsStub.CreateColumn(t.Ask, EFieldType.Double, 4, "Ask"),
                ColumnsStub.CreateColumn(t.BidSize, EFieldType.Int32, 5, "BidSize"),
                ColumnsStub.CreateColumn(t.AskSize, EFieldType.Int32, 6, "AskSize"),
                ColumnsStub.CreateColumn(t.Mode, EFieldType.String, 7, "Mode"),
                ColumnsStub.CreateColumn(t.Ex, EFieldType.String, 8, "Ex")
            };
            var bitset = new QuoteBitsetColumnStub(columns, GetNullsColumn(t).ToArray());
            return columns.Concat(new[] {bitset}).ToArray();
        } 

        private static IEnumerable<int> GetNullsColumn(Quote quote)
        {
            if (!quote.__isset.timestamp) yield return 0;
            if (!quote.__isset.sym) yield return 1;
            if (!quote.__isset.bid) yield return 2;
            if (!quote.__isset.ask) yield return 3;
            if (!quote.__isset.bidSize) yield return 4;
            if (!quote.__isset.askSize) yield return 5;
            if (!quote.__isset.mode) yield return 6;
            if (!quote.__isset.ex) yield return 7;
        }
    }
}