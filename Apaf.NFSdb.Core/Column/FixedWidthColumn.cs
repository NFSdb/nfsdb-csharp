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
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core.Column
{
    public class FixedColumn : IFixedWidthColumn
    {
        private readonly EFieldType _fieldType;
        private readonly int _sizeBytes;
        private readonly IRawFile _storage;

        public FixedColumn(IRawFile storage, EFieldType fieldType, string propertyName = null)
        {
            _storage = storage;
            _fieldType = fieldType;
            _sizeBytes = fieldType.GetSize();
            PropertyName = propertyName;
        }

        public int GetInt32(long rowID)
        {
            return _storage.ReadInt32(rowID * _sizeBytes);
        }

        public long GetInt64(long rowID)
        {
            return _storage.ReadInt64(rowID * _sizeBytes);
        }

        public short GetInt16(long rowID)
        {
            return _storage.ReadInt16(rowID * _sizeBytes);
        }

        public byte GetByte(long rowID)
        {
            return _storage.ReadByte(rowID * _sizeBytes);
        }

        public bool GetBool(long rowID)
        {
            return _storage.ReadBool(rowID * _sizeBytes);
        }

        public double GetDouble(long rowID)
        {
            return _storage.ReadDouble(rowID * _sizeBytes);
        }

        public unsafe DateTime GetDateTime(long rowID)
        {
            if (_fieldType == EFieldType.DateTimeEpochMs)
            {
                return DateUtils.UnixTimestampToDateTime(GetInt64(rowID));
            }
            long dateTimeData = GetInt64(rowID);
            long* l = &dateTimeData;
            return ((DateTime*)l)[0];
        }

        public void SetInt32(long rowID, int value)
        {
            var offset = rowID*_sizeBytes;
            _storage.WriteInt32(offset, value);
        }

        public void SetInt64(long rowID, long value)
        {
            var offset = rowID * _sizeBytes;
            _storage.WriteInt64(offset, value);
        }

        public void SetInt16(long rowID, short value)
        {
            var offset = rowID * _sizeBytes;
            _storage.WriteInt16(offset, value);
        }

        public void SetByte(long rowID, byte value)
        {
            var offset = rowID * _sizeBytes;
            _storage.WriteByte(offset, value);
        }

        public void SetBool(long rowID, bool value)
        {
            var offset = rowID * _sizeBytes;
            _storage.WriteBool(offset, value);
        }

        public void SetDouble(long rowID, double value)
        {
            var offset = rowID * _sizeBytes;
            _storage.WriteDouble(offset, value);
        }

        public void SetDateTime(long rowID, DateTime value)
        {
            var toLong = _fieldType == EFieldType.DateTimeEpochMs
                ? DateUtils.DateTimeToUnixTimeStamp(value)
                : DateUtils.ToUnspecifiedDateTicks(value);

            SetInt64(rowID, toLong);
        }

        public EFieldType FieldType
        {
            get { return _fieldType; }
        }

        public string PropertyName { get; private set; }

        int ITypedColumn<int>.Get(long rowID, ReadContext readContext)
        {
            return GetInt32(rowID);
        }

        long ITypedColumn<long>.Get(long rowID, ReadContext readContext)
        {
            return GetInt64(rowID);
        }

        short ITypedColumn<short>.Get(long rowID, ReadContext readContext)
        {
            return GetInt16(rowID);
        }

        byte ITypedColumn<byte>.Get(long rowID, ReadContext readContext)
        {
            return GetByte(rowID);
        }

        bool ITypedColumn<bool>.Get(long rowID, ReadContext readContext)
        {
            return GetBool(rowID);
        }

        double ITypedColumn<double>.Get(long rowID, ReadContext readContext)
        {
            return GetDouble(rowID);
        }

        DateTime ITypedColumn<DateTime>.Get(long rowID, ReadContext readContext)
        {
            return GetDateTime(rowID);
        }
    }
}