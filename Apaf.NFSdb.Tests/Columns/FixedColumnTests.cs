﻿using System;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Apaf.NFSdb.Tests.Tx;
using Moq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Columns
{
    [TestFixture]
    public class FixedColumnTests
    {
        private Mock<IRawFile> _mockStorage;

        [Test]
        public void ShouldReadByte()
        {
            var col = CreateCol(EFieldType.Byte, (byte)255);
            Assert.That(col.GetByte(1), Is.EqualTo(255));
        }

        [Test]
        public void ShouldReadInt32()
        {
            var col = CreateCol(EFieldType.Int32, int.MinValue);
            Assert.That(col.GetInt32(1), Is.EqualTo(int.MinValue));
        }

        [Test]
        public void ShouldReadInt64()
        {
            var col = CreateCol(EFieldType.Int64, long.MaxValue);
            Assert.That(col.GetInt64(1), Is.EqualTo(long.MaxValue));
        }

        [Test]
        public void ShouldReadBool()
        {
            var col = CreateCol(EFieldType.Bool, true);
            Assert.That(col.GetBool(1), Is.EqualTo(true));
        }

        [Test]
        public void ShouldReadInt16()
        {
            var col = CreateCol(EFieldType.Int16, short.MaxValue);
            Assert.That(col.GetInt16(1), Is.EqualTo(short.MaxValue));
        }

        [Test]
        public void ShouldReadDouble()
        {
            var col = CreateCol(EFieldType.Double, double.MaxValue);
            Assert.That(col.GetDouble(1), Is.EqualTo(double.MaxValue));
        }

        [TestCase(12.345)]
        [TestCase(double.MaxValue)]
        [TestCase(double.MinValue)]
        [TestCase(double.Epsilon)]
        [TestCase(double.NaN)]
        [TestCase(double.PositiveInfinity)]
        [TestCase(double.NegativeInfinity)]
        public void ShouldWriteDouble(double value)
        {
            var offset = DateTime.Now.Millisecond % 256;
            var col = CreateCol(EFieldType.Double);

            // Act.
            col.SetDouble(offset, value, TestTxLog.TestContext());

            Assert.That(col.GetDouble(offset), Is.EqualTo(value), "Offset " + offset);
            _mockStorage.Verify(m => m.WriteDouble(It.IsAny<long>(), It.IsAny<double>()), Times.Once);
        }

        [TestCase(124L)]
        [TestCase(long.MaxValue)]
        [TestCase(long.MinValue)]
        [TestCase(0L)]
        public void ShouldWriteInt64(long value)
        {
            var offset = DateTime.Now.Millisecond % 256;
            var col = CreateCol(EFieldType.Int64);
            col.SetInt64(offset, value, TestTxLog.TestContext());

            Assert.That(col.GetInt64(offset), Is.EqualTo(value), "Offset " + offset);
            _mockStorage.Verify(m => m.WriteInt64(It.IsAny<long>(), It.IsAny<long>()), Times.Once);
        }

        [TestCase(-124)]
        [TestCase(int.MaxValue)]
        [TestCase(int.MinValue)]
        [TestCase(0)]
        public void ShouldWriteInt32(int value)
        {
            var offset = DateTime.Now.Millisecond % 256;
            var col = CreateCol(EFieldType.Int32);
            col.SetInt32(offset, value, TestTxLog.TestContext());

            Assert.That(col.GetInt32(offset), Is.EqualTo(value), "Offset " + offset);
            _mockStorage.Verify(m => m.WriteInt32(It.IsAny<long>(), It.IsAny<int>()), Times.Once);
        }

        [TestCase(-2048)]
        [TestCase(short.MaxValue)]
        [TestCase(short.MinValue)]
        [TestCase(0)]
        public void ShouldWriteInt16(short value)
        {
            var offset = DateTime.Now.Millisecond % 256;
            var col = CreateCol(EFieldType.Int16);
            col.SetInt16(offset, value, TestTxLog.TestContext());

            Assert.That(col.GetInt16(offset), Is.EqualTo(value), "Offset " + offset);
            _mockStorage.Verify(m => m.WriteInt16(It.IsAny<long>(), It.IsAny<short>()), Times.Once);
        }

        [TestCase(234)]
        [TestCase(byte.MaxValue)]
        [TestCase(byte.MinValue)]
        public void ShouldWriteByte(byte value)
        {
            var offset = DateTime.Now.Millisecond % 256;
            var col = CreateCol(EFieldType.Byte);
            col.SetByte(offset, value, TestTxLog.TestContext());

            Assert.That(col.GetByte(offset), Is.EqualTo(value), "Offset " + offset);
            _mockStorage.Verify(m => m.WriteByte(It.IsAny<long>(), It.IsAny<byte>()), Times.Once);
        }

        [TestCase(EFieldType.Int32, 234)]
        [TestCase(EFieldType.Int64, -234L)]
        [TestCase(EFieldType.Int16, (short)268)]
        [TestCase(EFieldType.Byte, (byte)255)]
        [TestCase(EFieldType.Bool, false)]
        [TestCase(EFieldType.Double, 12.34)]
        public void Should_update_transaction_read_offset(EFieldType fieldType, object value)
        {
            // Init.
            var offset = DateTime.Now.Millisecond % 256;
            var col = CreateCol(fieldType);
            var tx = TestTxLog.TestContext();

            // Act.
            var setMethod = typeof(IFixedWidthColumn).GetMethod("Set" + fieldType);
            setMethod.Invoke(col, new object[] { offset, value, tx });

            // Verify.
            Assert.That(tx.PartitionTx[0].AppendOffset[0], Is.EqualTo((offset + 1) * fieldType.GetSize()));
        }

        [TestCase(true)]
        [TestCase(false)]
        public void ShouldWriteBool(bool value)
        {
            var offset = DateTime.Now.Millisecond % 256;
            var col = CreateCol(EFieldType.Bool);
            col.SetBool(offset, value, TestTxLog.TestContext());

            Assert.That(col.GetBool(offset), Is.EqualTo(value), "Offset " + offset);
            _mockStorage.Verify(m => m.WriteBool(It.IsAny<long>(), It.IsAny<bool>()), Times.Once);
        }

        private FixedColumn CreateCol(EFieldType eFieldType)
        {
             _mockStorage = RawFileStub.InMemoryFile();
            return new FixedColumn(_mockStorage.Object, eFieldType);
        }

        private FixedColumn CreateCol<T>(EFieldType eFieldType, T value)
        {
            var mockStorage = RawFileStub.RawFile(eFieldType, value);
            return new FixedColumn(mockStorage, eFieldType);
        }
    }
}