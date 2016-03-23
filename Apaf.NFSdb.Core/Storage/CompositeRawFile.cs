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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Exceptions;

namespace Apaf.NFSdb.Core.Storage
{
    public unsafe class CompositeRawFile : IRawFile
    {
        private readonly int _bitHint;
        private const byte TRUE = 1;
        private const byte FALSE = 0;
        public const int INITIAL_PARTS_COLLECTION_SIZE = 64;
        private IRawFilePart[] _buffers = new IRawFilePart[INITIAL_PARTS_COLLECTION_SIZE];
        private byte** _pointersArray;
        private long _pointersArrayLen;
        private long _pointersArrayFixedLen;

        private const long FILE_HEADER_LENGTH = MetadataConstants.FILE_HEADER_LENGTH;
        private const int ADDITIONAL_BUFFER_ARRAY_CAPACITY = 10;
        private ICompositeFile _compositeFile;
        private readonly object _buffSync = new object();
        private long _mappedSize;
        private bool _incompleteBufferMapped;

        public CompositeRawFile(string fileName,
            int bitHint,
            ICompositeFileFactory mmf,
            EFileAccess access,
            int partitionID,
            int fileID,
            int columnID,
            EDataType dataType)
            : this(fileName,
                bitHint,
                mmf,
                access,
                partitionID,
                fileID,
                columnID,
                dataType,
                MetadataConstants.MIN_FILE_BIT_HINT_NON_DATA)
        {
        }

        public CompositeRawFile(string fileName,
            int bitHint,
            ICompositeFileFactory mmf,
            EFileAccess access,
            int partitionID,
            int fileID,
            int columnID,
            EDataType dataType,
            int minBitHint)
        {
            if (bitHint < minBitHint
                || bitHint > MetadataConstants.MAX_FILE_BIT_HINT)
            {
                throw new NFSdbConfigurationException("Calclated size of file {0} " +
                                                      "is invalid. Should be >= 2^{1} and  <= 2^{2} " +
                                                      "but was 2^{3}",
                                                      fileName,
                                                      MetadataConstants.MIN_FILE_BIT_HINT,
                                                      MetadataConstants.MAX_FILE_BIT_HINT,
                                                      bitHint);
            }
            _bitHint = bitHint;
            Access = access;
            _compositeFile = mmf.OpenFile(fileName, bitHint, access);
            PartitionID = partitionID;
            FileID = fileID;
            ColumnID = columnID;
            DataType = dataType;
            Filename = fileName;
            _pointersArray = LongAllocate(INITIAL_PARTS_COLLECTION_SIZE);
            _pointersArrayLen = INITIAL_PARTS_COLLECTION_SIZE;
        }

        private static byte** LongAllocate(int size)
        {
            var ptr = (byte**)Marshal.AllocHGlobal(sizeof(byte*) * size);
            for (int i = 0; i < size; i++)
            {
                ptr[i] = null;
            }
            return ptr;
        }

        public void Dispose()
        {
            lock (_buffSync)
            {
                if (_buffers == null)
                {
                    return;
                }
                var buffCopy = _buffers;
                _buffers = null;

                foreach (var rawFilePart in buffCopy)
                {
                    if (rawFilePart != null)
                    {
                        rawFilePart.Dispose();
                    }
                }
                _buffers = null;
            }

            Marshal.FreeHGlobal((IntPtr)_pointersArray);

            _compositeFile.Dispose();
            _compositeFile = null;
            GC.SuppressFinalize(this);
        }

        public void Flush()
        {
            lock (_buffSync)
            {
                if (_buffers == null)
                {
                    return;
                }

                IRawFilePart[] buffCopy = _buffers;

                // Block concurrent reads.
                Thread.MemoryBarrier();
                _buffers = new IRawFilePart[0];
                Thread.MemoryBarrier();

                foreach (var rawFilePart in buffCopy)
                {
                    if (rawFilePart != null)
                    {
                        rawFilePart.Flush();
                    }
                }

                // Restore the flushed buffers.
                // Unblock reads.
                _buffers = buffCopy;
            }
        }

        ~CompositeRawFile()
        {
            if (_compositeFile != null) _compositeFile.Dispose();
        }

        public byte* GetFilePart(long offset)
        {
#if DEBUG
            if (offset < -FILE_HEADER_LENGTH)
            {
                throw new IndexOutOfRangeException("offset is " + offset);
            }
#endif
            var bufferIndex = ((offset + FILE_HEADER_LENGTH) >> _bitHint);

            // Check exists.
            if (bufferIndex < _pointersArrayFixedLen)
            {
                var ptr = _pointersArray[bufferIndex];
                if (ptr != null)
                {
                    return ptr + offset;
                }
            }

            return GetBufferCore(bufferIndex, offset);
        }

        public byte* GetBufferCore(long bufferIndex, long offset)
        {
            if (bufferIndex < _pointersArrayLen)
            {
                var view = _buffers[bufferIndex];
                if (view != null)
                {
                    var bufferSize = 1 << _bitHint;
                    var viewOffset = offset - bufferIndex * bufferSize + FILE_HEADER_LENGTH;
                    if (viewOffset < view.BufferSize)
                    {
                        return view.Pointer + viewOffset;
                    }

                    // The file should be re-mapped on higher level.
                    throw new NFSdbInvalidReadException(
                        "Attempt to read file {0} at offset {1} while it is mapped only to {2}",
                        Filename, view.BufferOffset + offset, view.BufferOffset + view.BufferSize);
                }
            }

            return MapBuffer(bufferIndex, offset);
        }

        private byte* MapBuffer(long bufferIndex, long offset)
        {
            lock (_buffSync)
            {
                if (bufferIndex < _pointersArrayLen)
                {
                    var ptr = _pointersArray[bufferIndex];
                    if (ptr != null)
                    {
                        return ptr + offset;
                    }
                }
                else
                {
                    // Add empty.
                    long newLen = bufferIndex + ADDITIONAL_BUFFER_ARRAY_CAPACITY;
                    var newBuffers = new IRawFilePart[newLen];
                    Array.Copy(_buffers, 0, newBuffers, 0, _pointersArrayLen);

                    var newPtrArray = LongAllocate((int) newLen);

                    AccessorHelper.Memcpy((byte*) newPtrArray, (byte*) _pointersArray, (int) (sizeof (byte*)*_pointersArrayLen));

                    var oldPtrArray = _pointersArray;

                    Thread.MemoryBarrier();
                    _pointersArray = newPtrArray;

                    Thread.MemoryBarrier();
                    _pointersArrayLen = newLen;
                    _pointersArrayFixedLen = newLen - 1;
                    _buffers = newBuffers;

                    Marshal.FreeHGlobal((IntPtr) oldPtrArray);
                }

                // Create.
                int bufferSize = 1 << _bitHint;
                long bufferOffset = bufferIndex*bufferSize;

                IRawFilePart view;

                var maxSize = _compositeFile.CheckSize();
                if (Access == EFileAccess.Read && bufferOffset + bufferSize > maxSize)
                {
                    if (maxSize < bufferOffset)
                    {
                        // Only last buffer can be incompletely mapped.
                        // The file should be re-mapped on higher level.
                        throw new NFSdbInvalidReadException(
                            "Attempt to read chunk in the middle of the file '{0}' with mismatched offset. File size {1}, buffer offset {2}.",
                            Filename, maxSize, bufferOffset);
                    }

                    if (_incompleteBufferMapped)
                    {
                        // Only last buffer can be incompletely mapped.
                        // The file should be re-mapped on higher level.
                        throw new NFSdbInvalidReadException(
                            "Attempt to read file '{0}' at {1}. Only one incomplete buffer allowed.",
                            Filename, bufferOffset + offset);
                    }

                    bufferSize = (int) (maxSize - bufferOffset);
                    _incompleteBufferMapped = true;
                }
                view = _compositeFile.CreateViewAccessor(bufferOffset, bufferSize);

                _buffers[bufferIndex] = view;
                var address = view.Pointer - view.BufferOffset + FILE_HEADER_LENGTH;

                Thread.MemoryBarrier();
                _pointersArray[bufferIndex] = address;

                Interlocked.Add(ref _mappedSize, bufferSize);
                return _pointersArray[bufferIndex] + offset;
            }
        }

        public int PartitionID { get; private set; }
        public int FileID { get; private set; }
        public int ColumnID { get; private set; }
        public string Filename { get; private set; }
        public EFileAccess Access { get; private set; }
        public long MappedSize { get { return _mappedSize; } }
        public EDataType DataType { get; private set; }

        public void ReadBytes(long offset, byte[] array, int arrayOffset, int sizeBytes)
        {
            fixed (byte* dst = array)
            {
                ReadBytes(offset, dst + arrayOffset, sizeBytes);
            }
        }

        public void ReadBytes(long offset, byte* array,  int sizeBytes)
        {
            while (sizeBytes > 0)
            {
                byte* ptr = GetFilePart(offset);
                //         Page size       - absolute offset % Page siez                          
                long end = (1 << _bitHint) - ((offset + FILE_HEADER_LENGTH) & ((1 << _bitHint) - 1));

                // Respect buffer size.
                var readSize1 = (int) Math.Min(end, sizeBytes);
                AccessorHelper.Memcpy(array, ptr, readSize1);
             
                offset += readSize1;
                sizeBytes -= readSize1;
                array += readSize1;
            }
        }


        public int ReadInt32(long offset)
        {
            return ((int*) GetFilePart(offset))[0];
        }

        public byte ReadByte(long offset)
        {
            return *(GetFilePart(offset));
        }

        public long ReadInt64(long offset)
        {
            return *((long*)GetFilePart(offset));
        }

        public bool ReadBool(long offset)
        {
            return *(GetFilePart(offset)) == TRUE;
        }

        public double ReadDouble(long offset)
        {
            return *((double*)GetFilePart(offset));
        }

        public short ReadInt16(long offset)
        {
            return *((short*)GetFilePart(offset));
        }

        public ushort ReadUInt16(long offset)
        {
            return *((ushort*)GetFilePart(offset));
        }

        public long GetAppendOffset()
        {
            return *((long*)GetFilePart(-FILE_HEADER_LENGTH));
        }

        public void SetAppendOffset(long value)
        {
            ((long*)GetFilePart(-FILE_HEADER_LENGTH))[0] = value;
        }

        public void WriteBytes(long offset, byte[] array, int arrayOffset, int sizeBytes)
        {
            fixed (byte* src = array)
            {
                WriteBytes(offset, src + arrayOffset, sizeBytes);
            }
        }

        public void WriteBytes(long offset, byte* array, int sizeBytes)
        {
            while (sizeBytes > 0)
            {
                byte* ptr = GetFilePart(offset);
                //         Page size       - absolute offset % Page siez                          
                long end = (1 << _bitHint) - ((offset + FILE_HEADER_LENGTH) & ((1 << _bitHint) - 1));

                // Respect buffer size.
                var readSize1 = (int)Math.Min(end, sizeBytes);
                AccessorHelper.Memcpy(ptr, array, readSize1);

                offset += readSize1;
                sizeBytes -= readSize1;
                array += readSize1;
            }
        }

        public void WriteInt64(long offset, long value)
        {
            ((long*)GetFilePart(offset))[0] = value;
        }

        public void WriteInt32(long offset, int value)
        {
            *((int*)GetFilePart(offset)) = value;
        }

        public void WriteInt16(long offset, short value)
        {
            *((short*)GetFilePart(offset)) = value;
        }

        public void WriteByte(long offset, byte value)
        {
            GetFilePart(offset)[0] = value;
        }

        public void WriteBool(long offset, bool value)
        {
            GetFilePart(offset)[0] = value ? TRUE : FALSE; 
        }

        public void WriteDouble(long offset, double value)
        {
            ((double*)GetFilePart(offset))[0] = value;
        }

        public void WriteUInt16(long offset, ushort value)
        {
            ((ushort*)GetFilePart(offset))[0] = value;
        }

        public override string ToString()
        {
            return "CompositeRawFile: " + Filename;
        }

        internal string GetAllUnsafePointers()
        {
            var sb = new StringBuilder();
            for (int i = 0; i < _pointersArrayLen; i++)
            {
                sb.Append(new IntPtr(_pointersArray[i]));
                sb.Append(";");
            }
            return sb.ToString();
        }

        internal string GetAllBufferPointers()
        {
            var sb = new StringBuilder();
            for (int i = 0; i < _pointersArrayLen; i++)
            {
                sb.Append(_buffers[i] != null ? new IntPtr(_buffers[i].Pointer) : new IntPtr());
                sb.Append(";");
            }
            return sb.ToString();
        }
    }
}