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
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
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
        public void ShouldReadDateTime()
        {

            var col = CreateCol(EFieldType.DateTime, 0L);
            Assert.That(col.GetDateTime(1), Is.EqualTo(DateTime.MinValue));
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
            col.SetDouble(offset, value);

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
            col.SetInt64(offset, value);

            Assert.That(col.GetInt64(offset), Is.EqualTo(value), "Offset " + offset);
            _mockStorage.Verify(m => m.WriteInt64(It.IsAny<long>(), It.IsAny<long>()), Times.Once);
        }

        [TestCaseSource("ShouldWriteDateTimeSource")]
        public void ShouldWriteDateTime(DateTime value)
        {
            var offset = DateTime.Now.Millisecond % 256;
            var col = CreateCol(EFieldType.Int64);
            col.SetDateTime(offset, value);

            DateTime result = col.GetDateTime(offset);
            Assert.That(result, Is.EqualTo(value), "Offset " + offset);
            _mockStorage.Verify(m => m.WriteInt64(It.IsAny<long>(), It.IsAny<long>()), Times.Once);
        }

        public IEnumerable<TestCaseData> ShouldWriteDateTimeSource()
        {
            yield return new TestCaseData(DateTime.MinValue);
            yield return new TestCaseData(DateTime.Now.Date);
            yield return new TestCaseData(DateTime.MaxValue);
        }

        [TestCase(-124)]
        [TestCase(int.MaxValue)]
        [TestCase(int.MinValue)]
        [TestCase(0)]
        public void ShouldWriteInt32(int value)
        {
            var offset = DateTime.Now.Millisecond % 256;
            var col = CreateCol(EFieldType.Int32);
            col.SetInt32(offset, value);

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
            col.SetInt16(offset, value);

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
            col.SetByte(offset, value);

            Assert.That(col.GetByte(offset), Is.EqualTo(value), "Offset " + offset);
            _mockStorage.Verify(m => m.WriteByte(It.IsAny<long>(), It.IsAny<byte>()), Times.Once);
        }
        
        [TestCase(true)]
        [TestCase(false)]
        public void ShouldWriteBool(bool value)
        {
            var offset = DateTime.Now.Millisecond % 256;
            var col = CreateCol(EFieldType.Bool);
            col.SetBool(offset, value);

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