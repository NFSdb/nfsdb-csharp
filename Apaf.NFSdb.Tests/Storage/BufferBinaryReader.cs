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
using System.IO;
using System.Runtime.InteropServices;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Tests.Storage
{
    public class BufferBinaryReader : IRawFilePart
    {
        private readonly byte[] _buffer;
        private readonly BinaryWriter _writer;

        public BufferBinaryReader(byte[] buffer, int? fileID = null)
        {
            _buffer = buffer;
            BufferOffset = 0;
            BufferSize = buffer.Length;
            _writer = new BinaryWriter(new MemoryStream(buffer));
            if (fileID.HasValue)
            {
                FileID = fileID.Value;
            }
        }

        public bool Disposed { get; private set; }

        public int PartitionID { get; private set; }
        public int FileID { get; private set; }
        public int ColumnID { get; private set; }
        public EDataType DataType { get; private set; }
        public EFileAccess Access { get; private set; }

        public string Filename
        {
            get { return "TestFile"; } 
        }

        public void ReadBytes(long offset, byte[] array, int arrayOffset, int sizeBytes)
        {
            Buffer.BlockCopy(_buffer, (int) offset, array, arrayOffset, sizeBytes);
        }

        public unsafe int ReadInt32(long offset)
        {
#if BIGENDIAN
            return _buffer[offset] << 24 | _buffer[offset + 1] << 16 | _buffer[offset + 2] << 8 | _buffer[offset + 3];
#else
            fixed (byte* ptr = _buffer)
            {
                return ((int*)(&ptr[offset]))[0];
            }
#endif
        }

        public byte ReadByte(long offset)
        {
            return _buffer[offset];
        }

        public unsafe long ReadInt64(long offset)
        {
#if BIGENDIAN
            int num = _buffer[offset++] << 24 | _buffer[offset++] << 16 | _buffer[offset++] << 8 | _buffer[offset++];
            return ((uint) (_buffer[offset++] << 24 | _buffer[offset++] << 16 | _buffer[offset++] << 8) |
                    _buffer[offset++]) |
                   ((long) num) << 32;
#else
            fixed (byte* ptr = _buffer)
            {
                return ((long*)(&ptr[offset]))[0];
            }
#endif
        }

        public bool ReadBool(long offset)
        {
            return _buffer[offset] > 0;
        }

        public double ReadDouble(long offset)
        {
            return BitConverter.Int64BitsToDouble(ReadInt64(offset));
        }

        public unsafe short ReadInt16(long offset)
        {
#if BIGENDIAN
            return (short) (_buffer[offset++] << 8 | _buffer[offset++]);
        #else
            fixed (byte* ptr = _buffer)
            {
                return ((short*)(&ptr[offset]))[0];
            }
#endif
        }

        public unsafe ushort ReadUInt16(long offset)
        {
#if BIGENDIAN
            return (ushort) (_buffer[offset++] << 8 | _buffer[offset++]);
#else
            fixed (byte* ptr = _buffer)
            {
                return ((ushort*)(&ptr[offset]))[0];
            }
#endif
        }

        public long GetAppendOffset()
        {
            throw new NotSupportedException();
        }

        public void SetAppendOffset(long value)
        {
            throw new NotSupportedException();
        }

        public long GetAppendOffset(ITransactionContext tx)
        {
            throw new NotSupportedException();
        }

        public void WriteBytes(long offset, byte[] array, int arrayOffset, int sizeBytes)
        {
            Buffer.BlockCopy(array, arrayOffset, _buffer, (int) offset, sizeBytes);
        }

        public unsafe void WriteBytes(long offset, byte* array, int arrayOffset, int sizeBytes)
        {
            for (int i = 0; i < sizeBytes; i++)
            {
                _buffer[offset + i] = array[arrayOffset + i];
            }
        }

        public void WriteInt64(long offset, long value)
        {
            _writer.Seek((int) offset, SeekOrigin.Begin);
#if BIGENDIAN
            value = IPAddress.HostToNetworkOrder(value);
#endif
            _writer.Write(value);
        }

        public void WriteInt32(long offset, int value)
        {
            _writer.Seek((int) offset, SeekOrigin.Begin);
#if BIGENDIAN
            value = IPAddress.HostToNetworkOrder(value);
#endif
            _writer.Write(value);
        }

        public void WriteInt16(long offset, short value)
        {
            _writer.Seek((int) offset, SeekOrigin.Begin);
#if BIGENDIAN
            value = IPAddress.HostToNetworkOrder(value);
#endif
            _writer.Write(value);
        }

        public void WriteByte(long offset, byte value)
        {
            _writer.Seek((int) offset, SeekOrigin.Begin);
            _writer.Write(value);
        }

        public void WriteBool(long offset, bool value)
        {
            _writer.Seek((int) offset, SeekOrigin.Begin);
            _writer.Write(value);
        }

        public void WriteDouble(long offset, double value)
        {
            long longVal = BitConverter.DoubleToInt64Bits(value);
            _writer.Seek((int) offset, SeekOrigin.Begin);
#if BIGENDIAN
            longVal = IPAddress.HostToNetworkOrder(longVal);
#endif
            _writer.Write(longVal);
        }

        public unsafe void WriteUInt16(long offset, uint value)
        {
            var uval = *(Int16*) &value;
            _writer.Seek((int)offset, SeekOrigin.Begin);
#if BIGENDIAN
            uval = IPAddress.HostToNetworkOrder(uval);
#endif
            _writer.Write(uval);
        }

        public void Flush()
        {
        }

        public void Dispose()
        {
            _writer.Dispose();
            Disposed = true;
        }

        public long BufferSize { get; private set; }
        public long BufferOffset { get; private set; }

        public unsafe void ReadChars(long offset, char[] address, int arrayOffset, int charlen)
        {
            fixed (byte* src = &_buffer[offset])
            {
                Marshal.Copy((IntPtr) src, address, arrayOffset, charlen);
            }
        }
    }
}