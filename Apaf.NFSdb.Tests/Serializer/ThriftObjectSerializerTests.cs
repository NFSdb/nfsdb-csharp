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
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Storage.Serializer;
using Apaf.NFSdb.Core.Tx;
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
            var deser = SerializeCircle(s);
            return (bool)typeof(Quote.Isset).GetField(propertyName)
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
            var deser = SerializeCircle(s);
            return (bool)typeof(Quote.Isset).GetField(propertyName)
                .GetValue(deser.__isset);
        }


        [Test]
        public void ShouldReadFirstInt64()
        {
            var deser = SerializeCircle(new Quote { Timestamp = 12345 });
            Assert.That(deser.Timestamp, Is.EqualTo(12345));
        }

        [Test]
        public void ShouldReadDouble()
        {
            var deser = SerializeCircle(new Quote { Bid = 0.12345 });
            Assert.That(deser.Bid, Is.EqualTo(0.12345));
        }

        private T SerializeCircle<T>(T item)
        {
            using (var j = ToJournal(item))
            {
                using (var r = j.OpenReadTx())
                {
                    return r.Items.First();
                }
            }
        }

        private static IJournal<T> ToJournal<T>(T item)
        {
            TestShared.Utils.ClearDir("ThriftSerializerFactoryTests");
            var j = new JournalBuilder()
                .WithAccess(EFileAccess.ReadWrite)
                .WithLocation("ThriftSerializerFactoryTests")
                .WithSerializerFactoryName(MetadataConstants.THRIFT_SERIALIZER_NAME)
                .ToJournal<T>();

            using (var wr = j.OpenWriteTx())
            {
                wr.Append(item);
                wr.Commit();
            }

            return j;
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
            serializer.Write(s, 0, new PartitionTxData(100, 1));
            
            // Verify.
            var resultCol = (IColumnStub)columns
                .Select(c => c.Column)
                .First(c => c.PropertyName == propertyName);
            return resultCol.Value;
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

        private ObjectSerializer CreateWriter(ColumnSource[] columns)
        {
            var serializerFactory = new ThriftSerializerFactory();
            serializerFactory.Initialize(typeof(Quote));

            return (ObjectSerializer)serializerFactory.CreateFieldSerializer(columns);
        }

        private static ColumnSource[] GetQuoteColumns(Quote t)
        {
            int i = 1;
            var columns = new[]
            {
               CreateSolumnSource(EFieldType.Int64, "Timestamp", t.Timestamp, i++),
               CreateSolumnSource(EFieldType.Symbol, "Sym", t.Sym, i++),
               CreateSolumnSource(EFieldType.Double, "Bid", t.Bid, i++),
               CreateSolumnSource(EFieldType.Double, "Ask", t.Ask, i++),
               CreateSolumnSource(EFieldType.Int32, "BidSize", t.BidSize, i++),
               CreateSolumnSource(EFieldType.Int32, "AskSize", t.AskSize, i++),
               CreateSolumnSource(EFieldType.String, "Mode", t.Mode, i++),
               CreateSolumnSource(EFieldType.String, "Ex", t.Ex, i++)
            };
            var bitset = BitSetColumnSource(columns);

            return columns.Concat(new[] {bitset}).ToArray();
        }

        private static ColumnSource BitSetColumnSource(ColumnSource[] columns)
        {
            var nullableCount = columns.Count(c => c.Metadata.Nullable);
            var bitsetColSource =
                ColumnMetadata.FromBitsetField(
                    new ColumnSerializerMetadata(EFieldType.BitSet, MetadataConstants.NULLS_FILE_NAME, null), nullableCount, columns.Length);

            var bitset =
                new ColumnSource(bitsetColSource,
                    new QuoteBitsetColumnStub(columns.Select(c => c.Column).ToArray(), new[] { 0, 2 }), columns.Length);
            return bitset;
        }

        private static ColumnSource CreateSolumnSource<T>(EFieldType type, string name, T value, int order)
        {
            ColumnMetadata colMeta;
            switch (type)
            {
                case EFieldType.Byte:
                case EFieldType.Bool:
                case EFieldType.Int16:
                case EFieldType.Int32:
                case EFieldType.Int64:
                case EFieldType.Double:
                case EFieldType.DateTime:
                case EFieldType.DateTimeEpochMs:
                    colMeta = ColumnMetadata.FromFixedField(new ColumnSerializerMetadata(type, name, null), order, order);
                    break;
                case EFieldType.Symbol:
                case EFieldType.String:
                    colMeta = ColumnMetadata.FromStringField(new ColumnSerializerMetadata(type, name, null), 10, 10, order, order);
                    break;
                case EFieldType.Binary:
                    colMeta = ColumnMetadata.FromBinaryField(new ColumnSerializerMetadata(type, name, null), 10, 10, order, order);
                    break;
                default:
                    throw new ArgumentOutOfRangeException("type");
            }
            return new ColumnSource(colMeta, ColumnsStub.CreateColumn(value, type, order, name), order);
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