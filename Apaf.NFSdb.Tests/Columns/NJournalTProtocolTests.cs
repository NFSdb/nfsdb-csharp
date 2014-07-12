//using Apaf.NFSdb.Core.Column;
//using Apaf.NFSdb.Core.Storage;
//using Apaf.NFSdb.Tests.Columns.ThriftModel;
//using NUnit.Framework;

//namespace Apaf.NFSdb.Tests.Columns
//{
//    [TestFixture]
//    public class NFSdbTProtocolTests
//    {
//        private NFSdbTProtocol CreateProtocol(Quote t)
//        {
//            var bitset = new QuoteBitsetColumnStub(t);
//            var columns = new[]
//            {
//                ColumnsStub.CreateColumn(t.Timestamp, EFieldType.Int64, 1),
//                ColumnsStub.CreateColumn(t.Sym, EFieldType.String, 2),
//                ColumnsStub.CreateColumn(t.Bid, EFieldType.Double, 3),
//                ColumnsStub.CreateColumn(t.Ask, EFieldType.Double, 4),
//                ColumnsStub.CreateColumn(t.BidSize, EFieldType.Int32, 5),
//                ColumnsStub.CreateColumn(t.AskSize, EFieldType.Int32, 6),
//                ColumnsStub.CreateColumn(t.Mode, EFieldType.String, 7),
//                ColumnsStub.CreateColumn(t.Ex, EFieldType.String, 8)
//            };
//            IReadContext readContext = null;
//            return new NFSdbTProtocol(bitset, columns, 0, readContext);
//        }

//        [Test]
//        public void ShouldReadDouble()
//        {
//            var serializedItem = CreateSerializedItem();

//            // Act.
//            NFSdbTProtocol p = CreateProtocol(serializedItem);
//            var deserializedItem = new Quote();
//            deserializedItem.Read(p);

//            // Verify.
//            Assert.AreEqual(serializedItem.Ask, deserializedItem.Ask);
//            Assert.AreEqual(serializedItem.Bid, deserializedItem.Bid);
//        }

//        [Test]
//        public void ShouldReadLong()
//        {
//            var serializedItem = CreateSerializedItem();

//            // Act.
//            NFSdbTProtocol p = CreateProtocol(serializedItem);
//            var deserializedItem = new Quote();
//            deserializedItem.Read(p);

//            // Verify.
//            Assert.AreEqual(serializedItem.Timestamp, deserializedItem.Timestamp);
//        }

//        [Test]
//        public void ShouldReadString()
//        {
//            var serializedItem = CreateSerializedItem();

//            // Act.
//            NFSdbTProtocol p = CreateProtocol(serializedItem);
//            var deserializedItem = new Quote();
//            deserializedItem.Read(p);

//            // Verify.
//            Assert.AreEqual(serializedItem.Sym, deserializedItem.Sym);
//        }

//        [Test]
//        public void ShouldReadInt()
//        {
//            var serializedItem = CreateSerializedItem();

//            // Act.
//            NFSdbTProtocol p = CreateProtocol(serializedItem);
//            var deserializedItem = new Quote();
//            deserializedItem.Read(p);

//            // Verify.
//            Assert.AreEqual(serializedItem.AskSize, deserializedItem.AskSize);
//            Assert.AreEqual(serializedItem.BidSize, deserializedItem.BidSize);
//        }

//        private static Quote CreateSerializedItem()
//        {
//            var serializedItem = new Quote
//            {
//                Ask = double.MinValue,
//                Bid = double.MaxValue,
//                Sym = "MST",
//                AskSize = int.MinValue,
//                BidSize = int.MaxValue,
//                Timestamp = long.MaxValue
//            };
//            return serializedItem;
//        }
//    }
//}