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
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public class BinaryColumn : IBinaryColumn
    {
        private const EFieldType INDEX_COLUMN_TYPE = EFieldType.Int64;
        private static readonly byte[] EMPTY_BYTE_ARRAY = new byte[0];
        public const int LARGE_HEADER_LENGTH = MetadataConstants.LARGE_VAR_COL_HEADER_LENGTH;
        private readonly IRawFile _data;
        protected readonly int HeaderSize;
        private readonly FixedColumn _indexColumn;
        private readonly int _maxSize;

        public BinaryColumn(IRawFile data, IRawFile index, 
            int maxSize, string propertyName)
        {
            PropertyName = propertyName;
            _data = data;
            _maxSize = maxSize;
            HeaderSize = LARGE_HEADER_LENGTH;
            _indexColumn = new FixedColumn(index, INDEX_COLUMN_TYPE);
        }

        public virtual EFieldType FieldType
        {
            get { return EFieldType.Binary; }
        }

        public virtual string PropertyName { get; private set; }

        public virtual int MaxSize
        {
            get { return _maxSize; }
        }

        public byte[] GetBytes(long rowID, IReadContext readContext)
        {
            var beginOffset = _indexColumn.GetInt64(rowID);
            var arrayLength = ReadLength(_data, beginOffset);
            if (arrayLength == 0)
            {
                return EMPTY_BYTE_ARRAY;
            }
            var byteArray = readContext.AllocateByteArray2(arrayLength);
            _data.ReadBytes(beginOffset + HeaderSize, byteArray, 0, arrayLength);
            return byteArray;
        }

        public unsafe int GetBytes(long rowID, byte* bytePtr, int startIndex, IReadContext readContext)
        {
            var beginOffset = _indexColumn.GetInt64(rowID);
            var arrayLength = ReadLength(_data, beginOffset);
            if (arrayLength == 0)
            {
                return 0;
            }

            _data.ReadBytes(beginOffset + HeaderSize, bytePtr, 0, arrayLength);
            return arrayLength;
        }

        public void SetBytes(long rowID, byte[] value, ITransactionContext tx)
        {
            if (value == null)
            {
                _indexColumn.SetInt64(rowID, MetadataConstants.INDEX_NULL_DATA_VALUE, tx);
            }
            else
            {
                var writeOffset = tx.PartitionTx[_data.PartitionID].AppendOffset[_data.FileID];
                _indexColumn.SetInt64(rowID, writeOffset, tx);
                var size = value.Length;
                WriteLength(writeOffset, size);
                _data.WriteBytes(writeOffset + HeaderSize, value, 0, size);
                tx.PartitionTx[_data.PartitionID].AppendOffset[_data.FileID] = 
                    writeOffset + HeaderSize + size;
            }
        }

        public unsafe void SetBytes(long rowID, byte* value, int startIndex, int length, ITransactionContext tx)
        {
            if (value == null)
            {
                _indexColumn.SetInt64(rowID, MetadataConstants.INDEX_NULL_DATA_VALUE, tx);
            }
            else
            {
                var writeOffset = tx.PartitionTx[_data.PartitionID].AppendOffset[_data.FileID];
                _indexColumn.SetInt64(rowID, writeOffset, tx);

                WriteLength(writeOffset, length);
                _data.WriteBytes(writeOffset + HeaderSize, value, startIndex, length);
                tx.PartitionTx[_data.PartitionID].AppendOffset[_data.FileID] = 
                    writeOffset + HeaderSize + length;
            }
        }

        protected virtual unsafe void WriteLength(long writeOffset, int size)
        {
            switch (HeaderSize)
            {
                case 1:
                    _data.WriteByte(writeOffset, (byte) size);
                    break;
                case 2:
                    var ussize = (UInt16) size;
                    _data.WriteBytes(writeOffset, (byte*)(&ussize), 0, 2);
                    break;
                case 4:
                    _data.WriteBytes(writeOffset, (byte*)(&size), 0, 4);
                    break;
                default:
                    throw new NotSupportedException("HeaderSize " + HeaderSize);
            }
        }

        protected virtual unsafe int ReadLength(IRawFile headerBuff, long offset)
        {
            switch (HeaderSize)
            {
                case 1:
                    return headerBuff.ReadByte(offset);
                case 2:
                    UInt16 ssize = 0;
                    headerBuff.ReadBytes(offset, (byte*)(&ssize), 0, 2);
                    return ssize;
                case 4:
                    int isize;
                    headerBuff.ReadBytes(offset, (byte*)(&isize), 0, 2);
                    return isize;
                default:
                    throw new NotSupportedException("HeaderSize " + HeaderSize);
            }
        }

        public object GetValue(long rowID, IReadContext readContext)
        {
            return GetBytes(rowID, readContext);
        }

        public void SetValue(long rowID, object value, ITransactionContext readContext)
        {
            SetBytes(rowID, (byte[])value, readContext);
        }
    }
}