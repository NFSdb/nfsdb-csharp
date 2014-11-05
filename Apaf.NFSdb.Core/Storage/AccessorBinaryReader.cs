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
using System.IO.MemoryMappedFiles;
using log4net;

namespace Apaf.NFSdb.Core.Storage
{
    public unsafe class AccessorBinaryReader : IRawFilePart
    {
        private readonly long _bufferOffset;
        private readonly long _bufferSize;
        private readonly MemoryMappedViewAccessor _view;
        private byte* _memoryPtr;
        private static readonly ILog LOG = LogManager.GetLogger(typeof (AccessorBinaryReader));
        private const byte TRUE = 1;
        private const byte FALSE = 0;
        private static readonly int ALLOCATION_GRANULARITY = AccessorHelper.Info.dwAllocationGranularity;

        public AccessorBinaryReader(MemoryMappedViewAccessor view, long bufferOffset,
            long bufferSize)
        {
            _view = view;
            _bufferOffset = bufferOffset;
            _bufferSize = bufferSize;
            ResolveMemoryPtr(view);
        }

        private void ResolveMemoryPtr(MemoryMappedViewAccessor view)
        {
            var num = _bufferOffset%ALLOCATION_GRANULARITY;
            view.SafeMemoryMappedViewHandle.AcquirePointer(ref _memoryPtr);
            _memoryPtr += num;
        }

        public long BufferSize
        {
            get { return _bufferSize; }
        }

        public long BufferOffset
        {
            get { return _bufferOffset; }
        }

        public int PartitionID
        {
            get { throw new NotSupportedException(); }
        }

        public int FileID
        {
            get { throw new NotSupportedException(); }
        }

        public int ColumnID
        {
            get { throw new NotSupportedException(); }
        }

        public EDataType DataType
        {
            get { throw new NotSupportedException(); }
        }

        public string Filename
        {
            get { throw new NotSupportedException(); }
        }

        public EFileAccess Access
        {
            get { throw new NotSupportedException(); }
        }

        public void ReadBytes(long offset, byte[] array, int arrayOffset, int sizeBytes)
        {
            var readPtr = _memoryPtr + offset - _bufferOffset;
            fixed (byte* dest = array)
            {
                AccessorHelper.Memcpy(dest + arrayOffset, readPtr, sizeBytes);
            }
        }

        public unsafe void ReadBytes(long offset, byte* array, int arrayOffset, int sizeBytes)
        {
            var readPtr = _memoryPtr + offset - _bufferOffset;
            AccessorHelper.Memcpy(array + arrayOffset, readPtr, sizeBytes);
        }

        public int ReadInt32(long offset)
        {
            var readPtr = (int*) (_memoryPtr + offset - _bufferOffset);
#if BIGENDIAN
            return IPAddress.HostToNetworkOrder(readPtr[0]);
#else
            return readPtr[0];
#endif
        }

        public byte ReadByte(long offset)
        {
            var readPtr = _memoryPtr + offset - _bufferOffset;
            return readPtr[0];
        }

        public long ReadInt64(long offset)
        {
            var readPtr = (long*) (_memoryPtr + offset - _bufferOffset);
#if BIGENDIAN
            return IPAddress.HostToNetworkOrder(readPtr[0]);
#else
            return readPtr[0];
#endif
        }

        public bool ReadBool(long offset)
        {
            var readPtr = _memoryPtr + offset - _bufferOffset;
            return readPtr[0] != 0;
        }

        public double ReadDouble(long offset)
        {
            var readPtr = (double*)(_memoryPtr + offset - _bufferOffset);
#if BIGENDIAN
            return IPAddress.HostToNetworkOrder(readPtr[0]);
#else
            return readPtr[0];
#endif
        }

        public short ReadInt16(long offset)
        {
            var readPtr = (Int16*) (_memoryPtr + offset - _bufferOffset);
#if BIGENDIAN
            return IPAddress.HostToNetworkOrder(readPtr[0]);
#else
            return readPtr[0];
#endif
        }

        public ushort ReadUInt16(long offset)
        {
            short ret = ReadInt16(offset);
            return *(ushort*) &(ret);
        }

        public long GetAppendOffset()
        {
            throw new NotSupportedException();
        }

        public void SetAppendOffset(long value)
        {
            throw new NotSupportedException();
        }

        public void WriteBytes(long offset, byte[] array, int arrayOffset, int sizeBytes)
        {
            var readPtr = _memoryPtr + offset - _bufferOffset;
            fixed (byte* dest = array)
            {
                AccessorHelper.Memcpy(readPtr, dest + arrayOffset, sizeBytes);
            }
        }

        public void WriteBytes(long offset, byte* array, int arrayOffset, int sizeBytes)
        {
            var readPtr = _memoryPtr + offset - _bufferOffset;
            AccessorHelper.Memcpy(readPtr, array + arrayOffset, sizeBytes);
        }

        public void WriteInt64(long offset, long value)
        {
            var readPtr = (long*) (_memoryPtr + offset - _bufferOffset);
#if BIGENDIAN
            readPtr[0] = IPAddress.HostToNetworkOrder(value);
#else
            readPtr[0] = value;
#endif
        }

        public void WriteInt32(long offset, int value)
        {
            var writePtr = (int*) (_memoryPtr + offset - _bufferOffset);
#if BIGENDIAN
            writePtr[0] = IPAddress.HostToNetworkOrder(value);
#else
            writePtr[0] = value;
#endif
        }

        public void WriteInt16(long offset, short value)
        {
            var writePtr = (short*) (_memoryPtr + offset - _bufferOffset);
#if BIGENDIAN
            writePtr[0] = IPAddress.HostToNetworkOrder(value);
#else
            writePtr[0] = value;
#endif
        }

        public void WriteByte(long offset, byte value)
        {
            var writePtr = _memoryPtr + offset - _bufferOffset;
            writePtr[0] = value;
        }

        public void WriteBool(long offset, bool value)
        {
            var writePtr = _memoryPtr + offset - _bufferOffset;
            writePtr[0] = value ? TRUE : FALSE;
        }

        public void WriteDouble(long offset, double value)
        {
            var writePtr = (double*) (_memoryPtr + offset - _bufferOffset);
#if BIGENDIAN
            writePtr[0] = IPAddress.HostToNetworkOrder(BitConverter.DoubleToInt64Bits(value));
#else
            writePtr[0] = value;
#endif
        }

        public void WriteUInt16(long offset, uint value)
        {
            WriteInt16(offset, *(short*) &(value));
        }

        public void Flush()
        {
            // Ptr to null before release
            // to avoid other threads using it.
            _memoryPtr = null;
            _view.SafeMemoryMappedViewHandle.ReleasePointer();

            ResolveMemoryPtr(_view);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void Dispose(bool disposed)
        {
            if (_memoryPtr != null)
            {
                _memoryPtr = null;
                _view.SafeMemoryMappedViewHandle.ReleasePointer();
                _view.Dispose();
            }

            if (disposed)
            {
                GC.SuppressFinalize(this);
            }
        }

        ~AccessorBinaryReader()
        {
            try
            {
                Dispose(false);
            }
            catch (Exception ex)
            {
                LOG.Error("Error in finilization thread.", ex);
            }
        }
    }
}