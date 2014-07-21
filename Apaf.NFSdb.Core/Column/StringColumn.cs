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
using System.Diagnostics;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public class StringColumn : IStringColumn
    {
        private const EFieldType INDEX_COLUMN_TYPE = EFieldType.Int64;
        public const int SHORT_HEADER_LENGTH = 1;
        public const int MEDIUM_HEADER_LENGTH = 2;
        public const int LARGE_HEADER_LENGTH = 4;
        public static readonly int BYTE_LIMIT = MetadataConstants.STRING_BYTE_LIMIT;
        public static readonly int TWO_BYTE_LIMIT = MetadataConstants.STRING_TWO_BYTE_LIMIT;
        private readonly IRawFile _data;
        private readonly IRawFile _index;
        private readonly int _headerSize;
        private readonly FixedColumn _indexColumn;
        private readonly int _maxSize;

        public StringColumn(IRawFile data, IRawFile index, 
            int maxSize, string propertyName)
        {
            PropertyName = propertyName;
            _data = data;
            _index = index;
            _maxSize = maxSize;
            _headerSize = LARGE_HEADER_LENGTH;
            _indexColumn = new FixedColumn(index, INDEX_COLUMN_TYPE);
        }

        public int MaxSize
        {
            get { return _maxSize; }
        }

        public unsafe virtual string GetString(long rowID, IReadContext readContext)
        {
            var beginOffset = GetOffset(rowID);
            var charlen = GetStringLength(_data, beginOffset, _headerSize);
            if (charlen == 0)
            {
                return string.Empty;
            }
            var byteArray = readContext.AllocateByteArray2(charlen * 2);
            _data.ReadBytes(beginOffset + _headerSize, byteArray, 0, charlen * 2);

#if BIGENDIAN
            fixed (byte* src = byteArray)
            {
                for (int i = 0; i < charlen * 2; i+=2)
                {
                    byte t = src[i];
                    src[i] = src[i + 1];
                    src[i + 1] = t;
                }
                var srcChar = (char*)src;
                var str = new string(srcChar, 0, charlen);
                return str;
            }
#else

            fixed (byte* src = byteArray)
            {
                var srcChar = (char*)src;
                return new string(srcChar, 0, charlen);
            }
#endif

        }

        public virtual unsafe void SetString(long rowID, string value, ITransactionContext tx)
        {
            if (value == null)
            {
                _indexColumn.SetInt64(rowID, MetadataConstants.STRING_NULL_VALUE, tx);
            }
            else
            {
                var stringOffset = tx.PartitionTx[_data.PartitionID].AppendOffset[_data.FileID];
                _indexColumn.SetInt64(rowID, stringOffset, tx);
                var charlen = value.Length;
                int size = charlen*2 + _headerSize;
#if BIGENDIAN
                var byteArray = tx.ReadCache.AllocateByteArray2(size);
                fixed (byte* src = byteArray)
                {
                    int pos = 0;
                    src[pos++] = 1;
                    switch (_headerSize - 1)
                    {
                        case 1:
                            src[pos++] = (byte)charlen;
                            break;
                        case 2:
                            var uLen = (ushort) charlen;
                            var shortBytes = (byte*)(&uLen);
                            src[pos++] = shortBytes[1];
                            src[pos++] = shortBytes[0];
                            break;
                        case 4:
                            var intBytes = (byte*)(&charlen);
                            src[pos++] = intBytes[3];
                            src[pos++] = intBytes[2];
                            src[pos++] = intBytes[1];
                            src[pos++] = intBytes[0];
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    fixed (char* chars = value)
                    {
                        var strBytes = (byte*) &chars[0];
                        for (int i = 0; i < charlen; i++)
                        {
                            src[pos++] = strBytes[2 * i + 1];
                            src[pos++] = strBytes[2 * i];
                        }
                    }
                    DebugCheckEquals(pos, size);
                }

                _data.WriteBytes(stringOffset, byteArray, 0, size);
#else
                switch (_headerSize)
                {
                    case 1:
                        _data.WriteByte(stringOffset, (byte)charlen);
                        break;
                    case 2:
                        var uLen = (ushort)charlen;
                        var shortBytes = (byte*)(&uLen);
                        _data.WriteBytes(stringOffset, shortBytes, 0, _headerSize);
                        break;
                    case 4:
                        var intBytes = (byte*)(&charlen);
                        _data.WriteBytes(stringOffset, intBytes, 0, _headerSize);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                fixed (char* chars = value)
                {
                    var strBytes = (byte*)chars;
                    _data.WriteBytes(stringOffset + _headerSize, strBytes, 0, charlen * 2);
                }
#endif
                tx.PartitionTx[_data.PartitionID].AppendOffset[_data.FileID] = stringOffset + size;
            }
        }

        [Conditional("DEBUG")]
        private void DebugCheckEquals(int pos, int size)
        {
            if (pos != size)
            {
                throw new NFSdbUnsafeDebugCheckException("Write string byte array size check failed");
            }
        }

        public EFieldType FieldType
        {
            get { return EFieldType.String; }
        }

        public string PropertyName { get; private set; }

        private static int GetHeaderSize(int maxSize)
        {
            if (maxSize <= BYTE_LIMIT) return SHORT_HEADER_LENGTH;
            if (maxSize <= TWO_BYTE_LIMIT) return MEDIUM_HEADER_LENGTH;
            return LARGE_HEADER_LENGTH;
        }

        private static int GetStringLength(IRawFile headerBuff, long offset, int headerSize)
        {
            switch (headerSize)
            {
                case 1:
                    return headerBuff.ReadByte(offset);
                case 2:
                    return headerBuff.ReadUInt16(offset);
                case 4:
                    return headerBuff.ReadInt32(offset);
                default:
                    throw new ArgumentOutOfRangeException("headerSize");
            }
        }

        private long GetOffset(long rowID)
        {
            return _indexColumn.GetInt64(rowID);
        }
    }
}