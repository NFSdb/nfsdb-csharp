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
using System.Reflection;
using Apaf.NFSdb.Core.Configuration;

namespace Apaf.NFSdb.Core.Column
{
    public class ColumnSerializerMetadata : IPocoClassSerializerMetadata
    {
        public ColumnSerializerMetadata(EFieldType type, string propertyName, 
            string fieldName, bool nullable = true, int size = 0)
        {
            DataType = type;
            PropertyName = propertyName;
            Size = size;
            Nullable = nullable;
            FieldName = fieldName;
        }

        public EFieldType DataType { get; set; }
        public string PropertyName { get; private set; }
        public int Size { get; private set; }
        public bool Nullable { get; private set; }
        public string FieldName { get; private set; }

        public MethodInfo GetGetMethod()
        {
            if (DataType == EFieldType.String
                || DataType == EFieldType.Symbol
                || DataType == EFieldType.Binary)
            {
                return typeof(IRefTypeColumn).GetMethod("GetValue");
            }
            EFieldType fieldType = DataType;

            // DateTimeEpochMs means DateTime field but stored as Epoch Ms.
            if (fieldType == EFieldType.DateTimeEpochMs)
            {
                fieldType = EFieldType.DateTime;
            }
            return typeof(IFixedWidthColumn).GetMethod("Get" + fieldType);
        }

        public string GetFileName()
        {
            return PropertyName.Substring(0, 1).ToLower() + PropertyName.Substring(1);
        }

        public MethodInfo GetSetMethod()
        {
            if (DataType == EFieldType.String
                || DataType == EFieldType.Symbol
                || DataType == EFieldType.Binary)
            {
                return typeof(IRefTypeColumn).GetMethod("SetValue");
            }
            EFieldType fieldType = DataType;
            // DateTimeEpochMs means DateTime field but stored as Epoch Ms.
            if (fieldType == EFieldType.DateTimeEpochMs)
            {
                fieldType = EFieldType.DateTime;
            }
            return typeof(IFixedWidthColumn).GetMethod("Set" + fieldType);
        }

        public Type GetDataType()
        {
            switch (DataType)
            {
                case EFieldType.Byte:
                case EFieldType.Bool:
                case EFieldType.Int16:
                case EFieldType.Int32:
                case EFieldType.Int64:
                case EFieldType.Double:
                case EFieldType.BitSet:
                    throw new ArgumentOutOfRangeException();
                case EFieldType.Symbol:
                case EFieldType.String:
                    return typeof (string);
                case EFieldType.Binary:
                    return typeof (byte[]);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public bool IsRefType()
        {
            return DataType == EFieldType.String
                   || DataType == EFieldType.Symbol
                   || DataType == EFieldType.Binary;
        }

        public FieldInfo GetNullableHasValueField()
        {
            var ntype = GetNullableType();
            return ntype.GetField("hasValue", BindingFlags.Instance | BindingFlags.NonPublic);
        }

        public FieldInfo GetNullableValueField()
        {
            var ntype = GetNullableType();
            return ntype.GetField("value", BindingFlags.Instance | BindingFlags.NonPublic);
        }

        private Type GetNullableType()
        {
            Type ntype;
            switch (DataType)
            {
                case EFieldType.Byte:
                    ntype = typeof(byte?);
                    break;
                case EFieldType.Bool:
                    ntype = typeof(bool?);
                    break;
                case EFieldType.Int16:
                    ntype = typeof(short?);
                    break;
                case EFieldType.Int32:
                    ntype = typeof(int?);
                    break;
                case EFieldType.Int64:
                    ntype = typeof(long?);
                    break;
                case EFieldType.Double:
                    ntype = typeof(double?);
                    break;
                case EFieldType.DateTime:
                case EFieldType.DateTimeEpochMs:
                    ntype = typeof(DateTime?);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            return ntype;
        }
    }
}