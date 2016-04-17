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
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Tests.Columns.ThriftModel
{

    public interface IColumnStub : IColumn
    {
        object Value { get; }
    }

    public static class ColumnsStub
    {
        public static IColumn CreateColumn<T>(T value, EFieldType fieldType, int fieldID, string propertyName = null)
        {
            return new ColumnStubImpl<T>(value, fieldType, propertyName);
        }

        public class ColumnStubImpl<T> : IFixedWidthColumn, IRefTypeColumn, IColumnStub
        {
            private T _value;

            public ColumnStubImpl(T value, EFieldType ft, string name = null)
            {
                _value = value;
                FieldType = ft;
                PropertyName = name;
            }

            public T1 GetValue<T1>(long rowID)
            {
                return _value != null ? (T1) (object) _value : default(T1);
            }

            public void SetValue<T1>(T1 value)
            {
                _value = (T)(object)value;
            }

            public int GetInt32(long rowID)
            {
                return GetValue<int>(rowID);
            }

            public long GetInt64(long rowID)
            {
                return GetValue<long>(rowID);
            }

            public short GetInt16(long rowID)
            {
                return GetValue<short>(rowID);
            }

            public byte GetByte(long rowID)
            {
                return GetValue<byte>(rowID);
            }

            public bool GetBool(long rowID)
            {
                return GetValue<bool>(rowID);
            }

            public double GetDouble(long rowID)
            {
                return GetValue<double>(rowID);
            }

            public DateTime GetDateTime(long rowID)
            {
                return GetValue<DateTime>(rowID);
            }

            public string GetString(long rowID, ReadContext readContext)
            {
                return GetValue<string>(rowID);
            }

            public void SetString(long rowID, string value, PartitionTxData readContext)
            {
                SetValue(value);
            }

            public object GetValue(long rowID, ReadContext readContext)
            {
                if (FieldType == EFieldType.String
                    || FieldType == EFieldType.Symbol)
                {
                    return GetString(rowID, readContext);
                }
                throw new System.NotImplementedException();
            }

            public void SetValue(long rowID, object value, PartitionTxData readContext)
            {
                if (FieldType == EFieldType.String
                    || FieldType == EFieldType.Symbol)
                {
                    SetString(rowID, (string) value, readContext);
                    return;
                }
                throw new System.NotImplementedException();
            }

            public void SetInt32(long rowID, int value)
            {
                SetValue(value);
            }

            public void SetInt64(long rowID, long value)
            {
                SetValue(value);
            }

            public void SetInt16(long rowID, short value)
            {
                SetValue(value);
            }

            public void SetByte(long rowID, byte value)
            {
                SetValue(value);
            }

            public void SetBool(long rowID, bool value)
            {
                SetValue(value);
            }

            public void SetDouble(long rowID, double value)
            {
                SetValue(value);
            }

            public void SetDateTime(long rowID, DateTime value)
            {
                SetValue(value);
            }

            public EFieldType FieldType { get; private set; }
            public string PropertyName { get; private set; }

            public object Value
            {
                get { return _value; }
            }

            int ITypedColumn<int>.Get(long rowID, ReadContext readContext)
            {
                return GetValue<int>(rowID);
            }

            long ITypedColumn<long>.Get(long rowID, ReadContext readContext)
            {
                return GetValue<long>(rowID);
            }

            short ITypedColumn<short>.Get(long rowID, ReadContext readContext)
            {
                return GetValue<short>(rowID);
            }

            byte ITypedColumn<byte>.Get(long rowID, ReadContext readContext)
            {
                return GetValue<byte>(rowID);
                throw new NotImplementedException();
            }

            bool ITypedColumn<bool>.Get(long rowID, ReadContext readContext)
            {
                return GetValue<bool>(rowID);
            }

            double ITypedColumn<double>.Get(long rowID, ReadContext readContext)
            {
                return GetValue<double>(rowID);
            }

            DateTime ITypedColumn<DateTime>.Get(long rowID, ReadContext readContext)
            {
                return GetValue<DateTime>(rowID);
            }
        }
    }
}