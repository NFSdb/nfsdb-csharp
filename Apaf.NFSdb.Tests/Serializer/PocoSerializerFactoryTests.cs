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
using System.Reflection;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Storage.Serializer;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Apaf.NFSdb.Tests.Tx;
using NUnit.Framework;
using Quote = Apaf.NFSdb.Tests.Columns.PocoModel.Quote;

namespace Apaf.NFSdb.Tests.Serializer
{
    [TestFixture]
    public class PocoSerializerFactoryTests
    {
        [TestCase(EFieldType.Bool, Result = "Bl")]
        [TestCase(EFieldType.String, Result = "Str")]
        [TestCase(EFieldType.Byte, Result = "Bt")]
        [TestCase(EFieldType.Int32, Result = "Int")]
        [TestCase(EFieldType.Int64, Result = "Lng")]
        [TestCase(EFieldType.Double, Result = "Dbl")]
        [TestCase(EFieldType.Int16, Result = "Int16")]
        public string Should_Detect_All_Column_Types(EFieldType fType)
        {
            var ojbType = new
            {
                Bl = true,
                Str = "string",
                Bt = (byte) 3,
                Int = 33,
                Lng = 430L,
                Dbl = 34.56,
                Int16 = (Int16) 987
            };

            var fact = CreatePocoSerializerFactory();
            fact.Initialize(ojbType.GetType());

            var cols = fact.ParseColumns();
            return cols.Single(c => c.DataType == fType).PropertyName;
        }

        [TestCase(EFieldType.Bool, Result = "Bl")]
        [TestCase(EFieldType.Byte, Result = "Bt")]
        [TestCase(EFieldType.Int32, Result = "Int")]
        [TestCase(EFieldType.Int64, Result = "Lng")]
        [TestCase(EFieldType.Double, Result = "Dbl")]
        [TestCase(EFieldType.Int16, Result = "Int16")]
        public string Should_Detect_All_Nullable_ColumnTypes(EFieldType fType)
        {
            var ojbType = new
            {
                Bl = (bool?)true,
                Bt = (byte?)3,
                Int = (int?)33,
                Lng = (long?)430L,
                Dbl = (double?)34.56,
                Int16 = (Int16?)987
            };

            var fact = CreatePocoSerializerFactory();
            fact.Initialize(ojbType.GetType());

            var cols = fact.ParseColumns();
            return cols.Single(c => c.DataType == fType).PropertyName;
        }

        [Test]
        public void Should_Calculate_Nullable_Count()
        {
            var ojbType = new
            {
                Str = "string",
                Bl = (bool?)true,
                Bt = (byte?)3,
                Bt2 = (byte)3,
                Int = (int?)33,
                Lng = (long?)430L,
                Dbl = (double?)34.56,
                Dbl2 = 34.56,
                Int16 = (Int16?)987
            };

            var fact = CreatePocoSerializerFactory();
            fact.Initialize(ojbType.GetType());

            var bitset = fact.ParseColumns().Single(c => c.DataType == EFieldType.BitSet);
            Assert.That(bitset.Size, Is.EqualTo(6));
        }

        [TestCase("Timestamp", ExpectedResult = 1309L)]
        [TestCase("Ask", ExpectedResult = 34.5)]
        [TestCase("Bid", ExpectedResult = 0.0)]
        [TestCase("AskSize", ExpectedResult = 134)]
        [TestCase("BidSize", ExpectedResult = 0)]
        [TestCase("Ex", ExpectedResult = "Ex1")]
        [TestCase("Mode", ExpectedResult = "")]
        [TestCase("Sym", ExpectedResult = null)]
        public object Should_write_object(string propertyName)
        {
            var testQuote = new Quote
            {
                Timestamp = 1309L,
                Ask = 34.5,
                Bid = null,
                AskSize = 134,
                BidSize = 0,
                Ex = "Ex1",
                Mode = ""
            };

            var quoteColumns = GetQuoteColumns(new Quote());
            var writer = CreateWriter<Quote>(quoteColumns);

            // Act.
            writer.Write(testQuote, 0, TestTxLog.TestContext());

            // Verify.
            var resultCol = (IColumnStub)quoteColumns.First(c => c.PropertyName == propertyName);
            return resultCol.Value;
        }

        public object Should_write_isnull(string propertyName)
        {
            var testQuote = new Quote
            {
                Timestamp = 1309L,
                Ask = 34.5,
                Bid = null,
                AskSize = 134,
                BidSize = 0,
                Ex = "Ex1",
                Mode = null
            };

            var quoteColumns = GetQuoteColumns(new Quote());
            var writer = CreateWriter<Quote>(quoteColumns);

            // Act.
            writer.Write(testQuote, 0, TestTxLog.TestContext());

            // Verify.
            var resultCol = (IColumnStub)quoteColumns.First(c => c.FieldType == EFieldType.BitSet);
            return resultCol.Value;
        }


        [TestCase("Timestamp", ExpectedResult = 1309L)]
        [TestCase("Ask", ExpectedResult = null)]
        [TestCase("Bid", ExpectedResult = 56.89)]
        [TestCase("AskSize", ExpectedResult = 134)]
        [TestCase("BidSize", ExpectedResult = 10)]
        [TestCase("Ex", ExpectedResult = "Ex1")]
        [TestCase("Mode", ExpectedResult = "")]
        [TestCase("Sym", ExpectedResult = null)]
        public object Should_read_object(string propertyName)
        {
            var testQuote = new Quote
            {
                Timestamp = 1309L,
                Ask = null,
                Bid = 56.89,
                AskSize = 134,
                BidSize = 10,
                Ex = "Ex1",
                Mode = ""
            };

            var columns = GetQuoteColumns(testQuote);
            var reader = CreateReader<Quote>(columns);

            // Act.
            var resultQuote = reader.Read(0, null);

            // Verify.
            return typeof (Quote).GetProperty(propertyName).GetGetMethod()
                .Invoke(resultQuote, null);
        }

        [Test]
        public void Should_write_null_columns()
        {
            var testQuote = new Quote
            {
                Timestamp = 1309L,
                Sym = null,
                Bid = null,
                Ask = 34.5,
                AskSize = 134,
                BidSize = 0,
                Mode = null,
                Ex = "Ex1",
            };

            var quoteColumns = GetQuoteColumns(new Quote());
            var writer = CreateWriter<Quote>(quoteColumns);

            // Act.
            writer.Write(testQuote, 0, TestTxLog.TestContext());

            // Verify.
            var bitsetCol = (QuoteBitsetColumnStub)quoteColumns.First(c => c.FieldType == EFieldType.BitSet);
            Assert.That(string.Join("|", bitsetCol.SetColumnIndecies), Is.EqualTo(
                "0|1|3"));
        }

        public void FullCycleWriteRead(string propertyName)
        {
            var testQuote = new Quote
            {
                Timestamp = 1309L,
                Ask = null,
                Bid = 56.89,
                AskSize = 134,
                BidSize = 10,
                Ex = "Ex1",
                Mode = ""
            };

            var quoteColumns = GetQuoteColumns(new Quote());
            var writer = CreateWriter<Quote>(quoteColumns);
            var reader = CreateReader<Quote>(quoteColumns);

            // Act.
            writer.Write(testQuote, 0, TestTxLog.TestContext());
            var resultQuote = reader.Read(0, null);

            var getProperty = typeof (Quote).GetProperty(propertyName).GetGetMethod();
            // Verify.
            Assert.That(getProperty.Invoke(resultQuote, null), Is.EqualTo(
                getProperty.Invoke(testQuote, null)));
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
                BidSize = 0,
                Ex = "qwerty",
                Mode = ""
            };
            var serializer = CreateWriter<Quote>(columns);

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
                Ask = 2.0,
                Bid = 4.5343,
                BidSize = 0,
                AskSize = 34,
                Ex = "qwerty",
                Mode = ""
            };

            var columns = GetQuoteColumns(s);
            var rdr = CreateReader<Quote>(columns);
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
        [Test]
        public void ShouldGetPropertyByFieldNameAnonymousType()
        {
            var anType = new {Name = "one", Doub = 1.0, Int = 1};
            var fact = CreatePocoSerializerFactory();
            fact.Initialize(anType.GetType());

            var fields = string.Join(",", anType.GetType().GetFields(BindingFlags.Instance
                                                                | BindingFlags.NonPublic)
                                                                .Select(f => fact.GetPropertyName(f.Name)));
            Assert.That(fields, Is.EqualTo("Name,Doub,Int"));
        }

        class MyClass
        {
            public string Name { get; set; }
            public double Doub { get; set; }
            public int Int { get; set; }
        }

        [Test]
        public void ShouldGetPropertyByFieldNameAutoField()
        {
            var anType = new MyClass { Name = "one", Doub = 1.0, Int = 1 };
            var fact = CreatePocoSerializerFactory();
            fact.Initialize(anType.GetType());

            var fields = string.Join(",", anType.GetType().GetFields(BindingFlags.Instance
                                                                | BindingFlags.NonPublic)
                                                                .Select(f => fact.GetPropertyName(f.Name)));
            Assert.That(fields, Is.EqualTo("Name,Doub,Int"));
        }


        class MyClass2
        {
            private string _name;
            private double _doub;
            private int _int;

            public string Name
            {
                get { return _name; }
                set { _name = value; }
            }

            public double Doub
            {
                get { return _doub; }
                set { _doub = value; }
            }

            public int Int
            {
                get { return _int; }
                set { _int = value; }
            }
        }

        [Test]
        public void ShouldGetPropertyByFieldNameNormalField()
        {
            var anType = new MyClass2 { Name = "one", Doub = 1.0, Int = 1 };
            var fact = CreatePocoSerializerFactory();
            fact.Initialize(anType.GetType());

            var fields = string.Join(",", anType.GetType().GetFields(BindingFlags.Instance
                                                                | BindingFlags.NonPublic)
                                                                .Select(f => fact.GetPropertyName(f.Name)));
            Assert.That(fields, Is.EqualTo("Name,Doub,Int"));
        }

        private PocoSerializerFactory CreatePocoSerializerFactory()
        {
            return new PocoSerializerFactory();
        }

        private PocoObjectSerializer CreateWriter<T>(IColumn[] columns)
        {
            var serializerFactory = new PocoSerializerFactory();
            serializerFactory.Initialize(typeof(T));

            return (PocoObjectSerializer)serializerFactory.CreateFieldSerializer(columns);
        }

        private PocoObjectSerializer CreateReader<T>(IColumn[] columns)
        {
            var serializerFactory = new PocoSerializerFactory();
            serializerFactory.Initialize(typeof(T));

            return (PocoObjectSerializer)serializerFactory.CreateFieldSerializer(columns);
        }

        private static IColumn[] GetQuoteColumns(Quote t)
        {
            var columns = new[]
            {
                ColumnsStub.CreateColumn(t.Timestamp, EFieldType.Int64, 1, "Timestamp"),
                ColumnsStub.CreateColumn(t.Sym, EFieldType.String, 2, "Sym"),
                ColumnsStub.CreateColumn(t.Bid ?? 0.0, EFieldType.Double, 3, "Bid"),
                ColumnsStub.CreateColumn(t.Ask ?? 0.0, EFieldType.Double, 4, "Ask"),
                ColumnsStub.CreateColumn(t.BidSize, EFieldType.Int32, 5, "BidSize"),
                ColumnsStub.CreateColumn(t.AskSize, EFieldType.Int32, 6, "AskSize"),
                ColumnsStub.CreateColumn(t.Mode, EFieldType.String, 7, "Mode"),
                ColumnsStub.CreateColumn(t.Ex, EFieldType.String, 8, "Ex")
            };
            var bitset = new QuoteBitsetColumnStub(columns, GetNullsColumn(t).ToArray());
            return columns.Concat(new[] { bitset }).ToArray();
        }

        private static IEnumerable<int> GetNullsColumn(Quote quote)
        {
            if (quote.Sym == null) yield return 0;
            if (!quote.Bid.HasValue) yield return 1;
            if (!quote.Ask.HasValue) yield return 2;
            if (quote.Mode == null) yield return 3;
            if (quote.Ex == null) yield return 4;
        }
    }
}