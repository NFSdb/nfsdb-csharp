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
using System.Linq;
using System.Reflection;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage.Serializer;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Apaf.NFSdb.Tests.Tx;
using NUnit.Framework;
using Quote = Apaf.NFSdb.Tests.Columns.PocoModel.Quote;
// ReSharper disable RedundantUsingDirective
using Apaf.NFSdb.Core.Storage;
using System.Diagnostics;
// ReSharper restore RedundantUsingDirective

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

            PocoSerializerFactory fact = CreatePocoSerializerFactory();
            fact.Initialize(ojbType.GetType());

            IList<IColumnSerializerMetadata> cols = fact.ParseColumns().ToList();
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
                Bl = (bool?) true,
                Bt = (byte?) 3,
                Int = (int?) 33,
                Lng = (long?) 430L,
                Dbl = (double?) 34.56,
                Int16 = (Int16?) 987
            };

            PocoSerializerFactory fact = CreatePocoSerializerFactory();
            fact.Initialize(ojbType.GetType());

            IList<IColumnSerializerMetadata> cols = fact.ParseColumns().ToList();
            return cols.Single(c => c.DataType == fType).PropertyName;
        }

        [Test]
        public void Should_Calculate_Nullable_Count()
        {
            var ojbType = new
            {
                Str = "string",
                Bl = (bool?) true,
                Bt = (byte?) 3,
                Bt2 = (byte) 3,
                Int = (int?) 33,
                Lng = (long?) 430L,
                Dbl = (double?) 34.56,
                Dbl2 = 34.56,
                Int16 = (Int16?) 987
            };

            PocoSerializerFactory fact = CreatePocoSerializerFactory();
            fact.Initialize(ojbType.GetType());

            IColumnSerializerMetadata bitset = fact.ParseColumns().Single(c => c.DataType == EFieldType.BitSet);
            Assert.That(bitset.Size, Is.EqualTo(7));
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

            ColumnSource[] quoteColumns = GetQuoteColumns(new Quote());
            PocoObjectSerializer writer = CreateWriter<Quote>(quoteColumns);

            // Act.
            writer.Write(testQuote, 0, TestTxLog.TestContext());

            // Verify.
            var resultCol = (IColumnStub) quoteColumns.Select(c => c.Column)
                .First(c => c.PropertyName == propertyName);
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

            ColumnSource[] quoteColumns = GetQuoteColumns(new Quote());
            PocoObjectSerializer writer = CreateWriter<Quote>(quoteColumns);

            // Act.
            writer.Write(testQuote, 0, TestTxLog.TestContext());

            // Verify.
            var resultCol = (IColumnStub) quoteColumns.Select(c => c.Column)
                .First(c => c.FieldType == EFieldType.BitSet);
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
        public object Should_read_anonymous_object(string propertyName)
        {
            var t = new
            {
                Timestamp = 1309L,
                Sym = (string) null,
                Bid = (double?) 56.89,
                Ask = (double?) null,
                BidSize = 10,
                AskSize = 134,
                Mode = "",
                Ex = "Ex1",
            };

            var columns = new[]
            {
                CreateSolumnSource(EFieldType.Int64, "Timestamp", t.Timestamp, 1),
                CreateSolumnSource(EFieldType.Symbol, "Sym", t.Sym, 2),
                CreateSolumnSource(EFieldType.Double, "Bid", t.Bid, 3),
                CreateSolumnSource(EFieldType.Double, "Ask", t.Ask ?? 0.0, 4),
                CreateSolumnSource(EFieldType.Int32, "BidSize", t.BidSize, 5),
                CreateSolumnSource(EFieldType.Int32, "AskSize", t.AskSize, 6),
                CreateSolumnSource(EFieldType.String, "Mode", t.Mode, 7),
                CreateSolumnSource(EFieldType.String, "Ex", t.Ex, 8)
            };
            var bitset =
                new ColumnSource(new ColumnSerializerMetadata(EFieldType.BitSet, MetadataConstants.NULLS_FILE_NAME, null),
                    new QuoteBitsetColumnStub(columns.Select(c => c.Column).ToArray(), new[] {0, 2}), 9);

            columns = columns.Concat(new[] {bitset}).ToArray();

            PocoObjectSerializer reader = CreateReader<Quote>(columns);

            // Act.
            object resultQuote = reader.Read(0, null);

            // Verify.
            return typeof (Quote).GetProperty(propertyName).GetGetMethod()
                .Invoke(resultQuote, null);
        }

        private static ColumnSource CreateSolumnSource<T>(EFieldType type, string name, T value, int order)
        {
            return new ColumnSource(new ColumnSerializerMetadata(type, name, null),
                ColumnsStub.CreateColumn(value, type, order, name), order);
        }

        private string AnonFieldName(string timestamp)
        {
            return null;
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

            ColumnSource[] columns = GetQuoteColumns(testQuote);
            PocoObjectSerializer reader = CreateReader<Quote>(columns);

            // Act.
            object resultQuote = reader.Read(0, null);

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

            ColumnSource[] quoteColumns = GetQuoteColumns(new Quote());
            PocoObjectSerializer writer = CreateWriter<Quote>(quoteColumns);

            // Act.
            writer.Write(testQuote, 0, TestTxLog.TestContext());

            // Verify.
            var bitsetCol = (QuoteBitsetColumnStub) quoteColumns.Select(c => c.Column)
                .First(c => c.FieldType == EFieldType.BitSet);
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

            ColumnSource[] quoteColumns = GetQuoteColumns(new Quote());
            PocoObjectSerializer writer = CreateWriter<Quote>(quoteColumns);
            PocoObjectSerializer reader = CreateReader<Quote>(quoteColumns);

            // Act.
            writer.Write(testQuote, 0, TestTxLog.TestContext());
            object resultQuote = reader.Read(0, null);

            MethodInfo getProperty = typeof (Quote).GetProperty(propertyName).GetGetMethod();
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
            PocoSerializerFactory fact = CreatePocoSerializerFactory();
            fact.Initialize(anType.GetType());

            string fields = string.Join(",", anType.GetType().GetFields(BindingFlags.Instance
                                                                        | BindingFlags.NonPublic)
                .Select(f => fact.GetPropertyName(f.Name)));
            Assert.That(fields, Is.EqualTo("Name,Doub,Int"));
        }

        private class MyClass
        {
            public string Name { get; set; }
            public double Doub { get; set; }
            public int Int { get; set; }
        }

        [Test]
        public void ShouldGetPropertyByFieldNameAutoField()
        {
            var anType = new MyClass {Name = "one", Doub = 1.0, Int = 1};
            PocoSerializerFactory fact = CreatePocoSerializerFactory();
            fact.Initialize(anType.GetType());

            string fields = string.Join(",", anType.GetType().GetFields(BindingFlags.Instance
                                                                        | BindingFlags.NonPublic)
                .Select(f => fact.GetPropertyName(f.Name)));
            Assert.That(fields, Is.EqualTo("Name,Doub,Int"));
        }


        private class MyClass2
        {
            public string Name { get; set; }

            public double Doub { get; set; }

            public int Int { get; set; }
        }

        [Test]
        public void ShouldGetPropertyByFieldNameNormalField()
        {
            var anType = new MyClass2 {Name = "one", Doub = 1.0, Int = 1};
            PocoSerializerFactory fact = CreatePocoSerializerFactory();
            fact.Initialize(anType.GetType());

            string fields = string.Join(",", anType.GetType().GetFields(BindingFlags.Instance
                                                                        | BindingFlags.NonPublic)
                .Select(f => fact.GetPropertyName(f.Name)));
            Assert.That(fields, Is.EqualTo("Name,Doub,Int"));
        }

        private PocoSerializerFactory CreatePocoSerializerFactory()
        {
            return new PocoSerializerFactory();
        }

        private PocoObjectSerializer CreateWriter<T>(ColumnSource[] columns)
        {
            var serializerFactory = new PocoSerializerFactory();
            serializerFactory.Initialize(typeof (T));

            return (PocoObjectSerializer) serializerFactory.CreateFieldSerializer(columns);
        }

        private PocoObjectSerializer CreateReader(Type t, ColumnSource[] columns)
        {
            var serializerFactory = new PocoSerializerFactory();
            serializerFactory.Initialize(t);

            return (PocoObjectSerializer) serializerFactory.CreateFieldSerializer(columns);
        }

        private PocoObjectSerializer CreateReader<T>(ColumnSource[] columns)
        {
            return CreateReader(typeof (T), columns);
        }

        private static ColumnSource[] GetQuoteColumns(Quote t)
        {
            var columns = new[]
            {
                CreateSolumnSource(EFieldType.Int64, "Timestamp", t.Timestamp, 1),
                CreateSolumnSource(EFieldType.Symbol, "Sym", t.Sym, 2),
                CreateSolumnSource(EFieldType.Double, "Bid", t.Bid ?? 0.0, 3),
                CreateSolumnSource(EFieldType.Double, "Ask", t.Ask ?? 0.0, 4),
                CreateSolumnSource(EFieldType.Int32, "BidSize", t.BidSize, 5),
                CreateSolumnSource(EFieldType.Int32, "AskSize", t.AskSize, 6),
                CreateSolumnSource(EFieldType.String, "Mode", t.Mode, 7),
                CreateSolumnSource(EFieldType.String, "Ex", t.Ex, 8)
            };
            var bitset =
                new ColumnSource(new ColumnSerializerMetadata(EFieldType.BitSet, MetadataConstants.NULLS_FILE_NAME, null),
                    new QuoteBitsetColumnStub(columns.Select(c => c.Column).ToArray(), GetNullsColumn(t).ToArray()), 9);
            return columns.Concat(new[] {bitset}).ToArray();
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