#region copyright
/*
 * Copyright (c) 2014. APAF (Alex Pelagenko).
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
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Tests.Storage;
using Moq;

namespace Apaf.NFSdb.Tests.Columns.ThriftModel
{
    public static class RawFileStub
    {
        public static IRawFile StringRawFile(byte[] header)
        {
            var rdr = new BufferBinaryReader(header);
            return rdr;
        }

        public static unsafe Mock<IRawFile> InMemoryFile(int bufferSize = 2048, int fileId = 0)
        {
            var core = new BufferBinaryReader(new byte[bufferSize]);
            var mock = new Mock<IRawFile>();
            mock.Setup(s => s.ReadBool(It.IsAny<long>())).Returns((long offset) => core.ReadBool(offset));
            mock.Setup(s => s.ReadByte(It.IsAny<long>())).Returns((long offset) => core.ReadByte(offset));
            mock.Setup(s => s.ReadInt32(It.IsAny<long>())).Returns((long offset) => core.ReadInt32(offset));
            mock.Setup(s => s.ReadInt64(It.IsAny<long>())).Returns((long offset) => core.ReadInt64(offset));
            mock.Setup(s => s.ReadInt16(It.IsAny<long>())).Returns((long offset) => core.ReadInt16(offset));
            mock.Setup(s => s.ReadUInt16(It.IsAny<long>())).Returns((long offset) => core.ReadUInt16(offset));
            mock.Setup(s => s.ReadDouble(It.IsAny<long>())).Returns((long offset) => core.ReadDouble(offset));
            mock.Setup(s => s.ReadBytes(It.IsAny<long>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>()))
                .Callback(
                    (long offset, byte[] buff, int arrayOff, int len) => core.ReadBytes(offset, buff, arrayOff, len));

            mock.Setup(s => s.WriteBool(It.IsAny<long>(), It.IsAny<bool>())).Callback((long offset, bool val) => core.WriteBool(offset, val));
            mock.Setup(s => s.WriteByte(It.IsAny<long>(), It.IsAny<byte>())).Callback((long offset, byte val) => core.WriteByte(offset, val));
            mock.Setup(s => s.WriteInt32(It.IsAny<long>(), It.IsAny<int>())).Callback((long offset, int val) => core.WriteInt32(offset, val));
            mock.Setup(s => s.WriteInt64(It.IsAny<long>(), It.IsAny<long>())).Callback((long offset, long val) => core.WriteInt64(offset, val));
            mock.Setup(s => s.WriteInt16(It.IsAny<long>(), It.IsAny<short>())).Callback((long offset, short val) => core.WriteInt16(offset, val));
            mock.Setup(s => s.WriteDouble(It.IsAny<long>(), It.IsAny<double>())).Callback((long offset, double val) => core.WriteDouble(offset, val));
            mock.Setup(s => s.WriteBytes(It.IsAny<long>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>()))
                .Callback(
                    (long offset, byte[] buff, int arrayOff, int len) => core.WriteBytes(offset, buff, arrayOff, len));

            mock.Setup(s => s.FileID).Returns(fileId);

            return mock;
        }

        public static IRawFile RawFile<T>(EFieldType eFieldType, T value)
        {
            var rdrMock = new Mock<IRawFile>();

            rdrMock.Setup(s => s.ReadBool(It.IsAny<long>())).Returns(() => (bool) (object) value);
            rdrMock.Setup(s => s.ReadByte(It.IsAny<long>())).Returns(() => (byte) (object) value);
            rdrMock.Setup(s => s.ReadInt32(It.IsAny<long>())).Returns(() => (int) (object) value);
            rdrMock.Setup(s => s.ReadInt64(It.IsAny<long>())).Returns(() => (long) (object) value);
            rdrMock.Setup(s => s.ReadInt16(It.IsAny<long>())).Returns(() => (short) (object) value);
            rdrMock.Setup(s => s.ReadDouble(It.IsAny<long>())).Returns(() => (double) (object) value);

            return rdrMock.Object;
        }
    }
}