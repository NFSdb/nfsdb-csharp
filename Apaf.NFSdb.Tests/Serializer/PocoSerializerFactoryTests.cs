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
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage.Serializer;
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
            IList<IColumnSerializerMetadata> cols = fact.Initialize(ojbType.GetType()).ToList();
            return cols.Single(c => c.ColumnType == fType).PropertyName;
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
            IList<IColumnSerializerMetadata> cols = fact.Initialize(ojbType.GetType()).ToList();
            return cols.Single(c => c.ColumnType == fType).PropertyName;
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
            IColumnSerializerMetadata bitset = fact.Initialize(ojbType.GetType()).Single(c => c.ColumnType == EFieldType.BitSet);
            Assert.That(bitset.Size, Is.EqualTo(7));
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
            

            // Act.
            object resultQuote = SerializeCircle(t);

            // Verify.
            return t.GetType().GetProperty(propertyName).GetGetMethod()
                .Invoke(resultQuote, null);
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
            TestShared.Utils.ClearDir("PocoSerializerFactoryTests");
            var j = new JournalBuilder()
                .WithAccess(EFileAccess.ReadWrite)
                .WithLocation("PocoSerializerFactoryTests")
                .WithSerializerFactoryName(MetadataConstants.POCO_SERIALIZER_NAME)
                .ToJournal<T>();

            using (var wr = j.OpenWriteTx())
            {
                wr.Append(item);
                wr.Commit();
            }

            return j;
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

            // Act.
            object resultQuote = SerializeCircle(testQuote);

            // Verify.
            return typeof (Quote).GetProperty(propertyName).GetGetMethod()
                .Invoke(resultQuote, null);
        }
     
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
    }
}