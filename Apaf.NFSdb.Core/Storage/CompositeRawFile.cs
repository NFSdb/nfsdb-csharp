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
using System.Threading;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Storage
{
    public class CompositeRawFile : IRawFile
    {
        private readonly int _bitHint;
        public const int INITIAL_PARTS_COLLECTION_SIZE = 64;
#if OPTIMIZE
        private AccessorBinaryReader[] _buffers = new AccessorBinaryReader[INITIAL_PARTS_COLLECTION_SIZE];
#else
        private IRawFilePart[] _buffers = new IRawFilePart[INITIAL_PARTS_COLLECTION_SIZE];
#endif
        private const long FILE_HEADER_LENGTH = MetadataConstants.FILE_HEADER_LENGTH;
        private ICompositeFile _compositeFile;
        private readonly object _buffSync = new object();

        public CompositeRawFile(string fileName,
            int bitHint,
            ICompositeFileFactory mmf,
            EFileAccess access,
            int partitionID,
            int fileID,
            int columnID,
            EDataType dataType)
        {
            _bitHint = bitHint;
            Access = access;
            _compositeFile = mmf.OpenFile(fileName, bitHint, access);
            PartitionID = partitionID;
            FileID = fileID;
            ColumnID = columnID;
            DataType = dataType;
            Filename = fileName;
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

#if OPTIMIZE
                AccessorBinaryReader[] buffCopy = _buffers;
#else
                IRawFilePart[] buffCopy = _buffers;
#endif

                // Block concurrent reads.
                Thread.MemoryBarrier();
#if OPTIMIZE
                _buffers = new AccessorBinaryReader[0];
#else
                _buffers = new IRawFilePart[0];
#endif
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

#if OPTIMIZE
        public AccessorBinaryReader GetFilePart(long offset)
#else
        public IRawFilePart GetFilePart(long offset)
#endif
        {
            var bufferIndex = (int)(offset >> _bitHint);

            // Check exists.
            if (bufferIndex < _buffers.Length)
            {
                var buffer = _buffers[bufferIndex];
                if (buffer != null)
                {
                    return buffer;
                }
            }

            lock (_buffSync)
            {
                if (bufferIndex < _buffers.Length)
                {
                    var buffer = _buffers[bufferIndex];
                    if (buffer != null)
                    {
                        return buffer;
                    }
                }
                else
                {
                    // Add empty.
#if OPTIMIZE
                    var newBuffer = new AccessorBinaryReader[bufferIndex + 10];
#else
                    var newBuffer = new IRawFilePart[bufferIndex + 10];
#endif
                    Array.Copy(_buffers, 0, newBuffer, 0, _buffers.Length);
                    Thread.MemoryBarrier();
                    _buffers = newBuffer;
                    Thread.MemoryBarrier();
                }

                // Create.
                int bufferSize = 1 << _bitHint;
                long bufferOffset = bufferIndex * (long) bufferSize;

#if OPTIMIZE
                var view = (AccessorBinaryReader)_compositeFile.CreateViewAccessor(bufferOffset, bufferSize);
#else
                var view = _compositeFile.CreateViewAccessor(bufferOffset, bufferSize);
#endif
                _buffers[bufferIndex] = view;
                return view;
            }
        }

        public int PartitionID { get; private set; }
        public int FileID { get; private set; }
        public int ColumnID { get; private set; }
        public string Filename { get; private set; }
        public EFileAccess Access { get; private set; }
        public EDataType DataType { get; private set; }

        public void ReadBytes(long offset, byte[] array, int arrayOffset, int sizeBytes)
        {
            offset += FILE_HEADER_LENGTH;
            var buff1 = GetFilePart(offset);

            // Respect buff1 size.
            var readSize1 = Math.Min((int)(buff1.BufferSize + buff1.BufferOffset - offset),
                sizeBytes);
            buff1.ReadBytes(offset, array, arrayOffset, readSize1);

            // Split.
            if (readSize1 < sizeBytes)
            {
                offset = buff1.BufferOffset + buff1.BufferSize - FILE_HEADER_LENGTH;
                sizeBytes -= readSize1;
                arrayOffset += readSize1;

                // Recursivly read.
                ReadBytes(offset, array, arrayOffset, sizeBytes);
            }
        }


        public int ReadInt32(long offset)
        {
            offset += FILE_HEADER_LENGTH;
            return GetFilePart(offset).ReadInt32(offset);
        }

        public byte ReadByte(long offset)
        {
            offset += FILE_HEADER_LENGTH;
            return GetFilePart(offset).ReadByte(offset);
        }

        public long ReadInt64(long offset)
        {
            offset += FILE_HEADER_LENGTH;
            var bufferIndex = (int)(offset >> _bitHint);

#if OPTIMIZE
            AccessorBinaryReader buffer = null;
#else
            IRawFilePart buffer = null;
#endif
            // Check exists.
            if (bufferIndex < _buffers.Length)
            {
                buffer = _buffers[bufferIndex];
            }

            if (buffer == null)
            {
                buffer = GetFilePart(offset);
            }
            return buffer.ReadInt64(offset);
        }

        public bool ReadBool(long offset)
        {
            offset += FILE_HEADER_LENGTH;
            return GetFilePart(offset).ReadBool(offset);
        }

        public double ReadDouble(long offset)
        {
            offset += FILE_HEADER_LENGTH;
            return GetFilePart(offset).ReadDouble(offset);
        }

        public short ReadInt16(long offset)
        {
            offset += FILE_HEADER_LENGTH;
            return GetFilePart(offset).ReadInt16(offset);
        }

        public ushort ReadUInt16(long offset)
        {
            offset += FILE_HEADER_LENGTH;
            return GetFilePart(offset).ReadUInt16(offset);
        }

        public long GetAppendOffset()
        {
            return GetFilePart(0).ReadInt64(0);
        }

        public void SetAppendOffset(long value)
        {
            GetFilePart(0).WriteInt64(0, value);
        }

        public void WriteBytes(long offset, byte[] array, int arrayOffset, int sizeBytes)
        {
            offset += FILE_HEADER_LENGTH;
            var buff1 = GetFilePart(offset);

            // Respect buff1 size.
            var writeSize = Math.Min((int)(buff1.BufferSize + buff1.BufferOffset - offset),
                sizeBytes);
            buff1.WriteBytes(offset, array, arrayOffset, writeSize);

            // Split.
            if (writeSize < sizeBytes)
            {
                offset = buff1.BufferOffset + buff1.BufferSize - FILE_HEADER_LENGTH;
                sizeBytes -= writeSize;
                arrayOffset += writeSize;

                // Recursivly read.
                WriteBytes(offset, array, arrayOffset, sizeBytes);
            }
        }

        public unsafe void WriteBytes(long offset, byte* array, int arrayOffset, int sizeBytes)
        {
            offset += FILE_HEADER_LENGTH;
            var buff1 = GetFilePart(offset);

            // Respect buff1 size.
            var writeSize = Math.Min((int)(buff1.BufferSize + buff1.BufferOffset - offset),
                sizeBytes);
            buff1.WriteBytes(offset, array, arrayOffset, writeSize);

            // Split.
            if (writeSize < sizeBytes)
            {
                offset = buff1.BufferOffset + buff1.BufferSize - FILE_HEADER_LENGTH;
                sizeBytes -= writeSize;
                arrayOffset += writeSize;

                // Recursivly read.
                WriteBytes(offset, array, arrayOffset, sizeBytes);
            }
        }

        public void WriteInt64(long offset, long value)
        {
            offset += FILE_HEADER_LENGTH;
            var bufferIndex = (int)(offset >> _bitHint);

#if OPTIMIZE
            AccessorBinaryReader buffer = null;
#else
            IRawFilePart buffer = null;
#endif
            // Check exists.
            if (bufferIndex < _buffers.Length)
            {
                buffer = _buffers[bufferIndex];
            }

            if (buffer == null)
            {
                buffer = GetFilePart(offset);
            }
            buffer.WriteInt64(offset, value);
        }

        public void WriteInt32(long offset, int value)
        {
            offset = offset + FILE_HEADER_LENGTH;
            var bufferIndex = (int)(offset >> _bitHint);

#if OPTIMIZE
            AccessorBinaryReader buffer = null;
#else
            IRawFilePart buffer = null;
#endif
            // Check exists.
            if (bufferIndex < _buffers.Length)
            {
                buffer = _buffers[bufferIndex];
            }

            if (buffer == null)
            {
                buffer = GetFilePart(offset);
            }
            buffer.WriteInt32(offset, value);
        }

        public void WriteInt16(long offset, short value)
        {
            offset = offset + FILE_HEADER_LENGTH;
            var bufferIndex = (int)(offset >> _bitHint);

#if OPTIMIZE
            AccessorBinaryReader buffer = null;
#else
            IRawFilePart buffer = null;
#endif
            // Check exists.
            if (bufferIndex < _buffers.Length)
            {
                buffer = _buffers[bufferIndex];
            }

            if (buffer == null)
            {
                buffer = GetFilePart(offset);
            }
            buffer.WriteInt16(offset, value);
        }

        public void WriteByte(long offset, byte value)
        {
            offset = offset + FILE_HEADER_LENGTH;
            var bufferIndex = (int)(offset >> _bitHint);

#if OPTIMIZE
            AccessorBinaryReader buffer = null;
#else
            IRawFilePart buffer = null;
#endif
            // Check exists.
            if (bufferIndex < _buffers.Length)
            {
                buffer = _buffers[bufferIndex];
            }

            if (buffer == null)
            {
                buffer = GetFilePart(offset);
            }
            buffer.WriteByte(offset, value);
        }

        public void WriteBool(long offset, bool value)
        {
            offset = offset + FILE_HEADER_LENGTH;
            var bufferIndex = (int)(offset >> _bitHint);

#if OPTIMIZE
            AccessorBinaryReader buffer = null;
#else
            IRawFilePart buffer = null;
#endif
            // Check exists.
            if (bufferIndex < _buffers.Length)
            {
                buffer = _buffers[bufferIndex];
            }

            if (buffer == null)
            {
                buffer = GetFilePart(offset);
            }
            buffer.WriteBool(offset, value);
        }

        public void WriteDouble(long offset, double value)
        {
            offset = offset + FILE_HEADER_LENGTH;
            var bufferIndex = (int)(offset >> _bitHint);

#if OPTIMIZE
            AccessorBinaryReader buffer = null;
#else
            IRawFilePart buffer = null;
#endif
            // Check exists.
            if (bufferIndex < _buffers.Length)
            {
                buffer = _buffers[bufferIndex];
            }

            if (buffer == null)
            {
                buffer = GetFilePart(offset);
            }
            buffer.WriteDouble(offset, value);
        }

        public void WriteUInt16(long offset, uint value)
        {
            offset = offset + FILE_HEADER_LENGTH;
            var bufferIndex = (int)(offset >> _bitHint);

#if OPTIMIZE
            AccessorBinaryReader buffer = null;
#else
            IRawFilePart buffer = null;
#endif
            // Check exists.
            if (bufferIndex < _buffers.Length)
            {
                buffer = _buffers[bufferIndex];
            }

            if (buffer == null)
            {
                buffer = GetFilePart(offset);
            }
            buffer.WriteUInt16(offset, value);
        }

        public override string ToString()
        {
            return "CompositeRawFile: " + Filename;
        }
    }
}