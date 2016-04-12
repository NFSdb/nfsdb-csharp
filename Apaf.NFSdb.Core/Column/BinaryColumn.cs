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

using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public class BinaryColumn : IBinaryColumn
    {
        private static readonly byte[] EMPTY_BYTE_ARRAY = new byte[0];
        private readonly IRawFile _data;
        private readonly int _maxSize;
        private readonly IRawFile _index;
        public const int HEADER_SIZE = MetadataConstants.LARGE_VAR_COL_HEADER_LENGTH;

        public BinaryColumn(IRawFile data, IRawFile index, 
            int maxSize, string propertyName)
        {
            PropertyName = propertyName;
            _data = data;
            _maxSize = maxSize;
            _index = index;
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
            var beginOffset = _index.ReadInt64(rowID * MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE);
            if (beginOffset == MetadataConstants.INDEX_NULL_DATA_VALUE)
            {
                return null;
            }
            var arrayLength = ReadLength(_data, beginOffset);
            if (arrayLength == 0)
            {
                return EMPTY_BYTE_ARRAY;
            }
            //var byteArray = readContext.AllocateByteArray2(arrayLength);
            var byteArray = new byte[arrayLength];
            _data.ReadBytes(beginOffset + HEADER_SIZE, byteArray, 0, arrayLength);
            return byteArray;
        }

        public unsafe int GetBytes(long rowID, byte* bytePtr, int startIndex, IReadContext readContext)
        {
            var beginOffset = _index.ReadInt64(rowID * MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE);
            if (beginOffset == MetadataConstants.INDEX_NULL_DATA_VALUE)
            {
                return MetadataConstants.INDEX_NULL_DATA_VALUE;
            }
            var arrayLength = ReadLength(_data, beginOffset);
            if (arrayLength == 0)
            {
                return 0;
            }

            _data.ReadBytes(beginOffset + HEADER_SIZE, bytePtr, arrayLength);
            return arrayLength;
        }

        public void SetBytes(long rowID, byte[] value, ITransactionContext readContext)
        {
            throw new System.NotImplementedException();
        }

        public void SetBytes(long rowID, byte[] value, PartitionTxData tx)
        {
            var offset = rowID * MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE;
            if (value != null)
            {
                var writeOffset = tx.AppendOffset[_data.FileID];
                _index.WriteInt64(offset, writeOffset);

                var size = value.Length;
                WriteLength(writeOffset, size);
                _data.WriteBytes(writeOffset + HEADER_SIZE, value, 0, size);
                tx.AppendOffset[_data.FileID] = writeOffset + HEADER_SIZE + size;
            }
            else
            {
                _index.WriteInt64(offset, MetadataConstants.INDEX_NULL_DATA_VALUE);
            }
        }

        public unsafe void SetBytes(long rowID, byte* value, int length, ITransactionContext tx)
        {
            var offset = rowID * MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE;
            if (value != null)
            {
                var writeOffset = tx.GetPartitionTx().AppendOffset[_data.FileID];
                _index.WriteInt64(offset, writeOffset);

                WriteLength(writeOffset, length);
                _data.WriteBytes(writeOffset + MetadataConstants.LARGE_VAR_COL_HEADER_LENGTH, value, length);
                tx.GetPartitionTx().AppendOffset[_data.FileID] = writeOffset + MetadataConstants.LARGE_VAR_COL_HEADER_LENGTH + length;
            }
            else
            {
                _index.WriteInt64(offset, MetadataConstants.INDEX_NULL_DATA_VALUE);
            }
        }

        public unsafe void SetBytes(long rowID, byte* value, int length, PartitionTxData tx)
        {
            var offset = rowID * MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE;
            if (value != null)
            {
                var writeOffset = tx.AppendOffset[_data.FileID];
                _index.WriteInt64(offset, writeOffset);
                tx.AppendOffset[_index.FileID] = offset + MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE;

                WriteLength(writeOffset, length);
                _data.WriteBytes(writeOffset + HEADER_SIZE, value, length);
                tx.AppendOffset[_data.FileID] = writeOffset + HEADER_SIZE + length;
            }
            else
            {
                _index.WriteInt64(offset, MetadataConstants.INDEX_NULL_DATA_VALUE);
                tx.AppendOffset[_index.FileID] = offset + MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE;
            }
        }

        protected virtual unsafe void WriteLength(long writeOffset, int size)
        {
            _data.WriteBytes(writeOffset, (byte*)(&size), 4);
        }

        protected virtual unsafe int ReadLength(IRawFile headerBuff, long offset)
        {
            int isize;
            headerBuff.ReadBytes(offset, (byte*) (&isize), 2);
            return isize;
        }

        public virtual object GetValue(long rowID, IReadContext readContext)
        {
            return GetBytes(rowID, readContext);
        }

        public virtual void SetValue(long rowID, object value, PartitionTxData readContext)
        {
            SetBytes(rowID, (byte[])value, readContext);
        }

        public byte[] Get(long rowID, IReadContext readContext)
        {
            return GetBytes(rowID, readContext);
        }
    }
}