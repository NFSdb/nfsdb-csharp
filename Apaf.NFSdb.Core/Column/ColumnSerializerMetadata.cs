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
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage.Serializer;

namespace Apaf.NFSdb.Core.Column
{
    public class ColumnSerializerMetadata : IPocoClassSerializerMetadata
    {
        private readonly IColumnDataType _columnDataType;

        public ColumnSerializerMetadata(EFieldType fieldType, string propertyName, 
            string fieldName, bool nullable = true, int size = 0)
        {
            _columnDataType = JournalColumnRegistry.Instance.GetSerializer(fieldType);
            ColumnType = _columnDataType.ColumnType;
            PropertyName = propertyName;
            Size = size;
            Nullable = nullable;
            FieldName = fieldName;
            DataType = _columnDataType.Clazz;
        }

        public EFieldType ColumnType { get; set; }
        public Type DataType { get; private set; }
        public string PropertyName { get; private set; }
        public int Size { get; private set; }
        public bool Nullable { get; private set; }
        public string FieldName { get; private set; }

        public MethodInfo GetGetMethod()
        {
            if (ColumnType == EFieldType.String
                || ColumnType == EFieldType.Symbol
                || ColumnType == EFieldType.Binary)
            {
                return typeof(IRefTypeColumn).GetMethod("GetValue");
            }
            EFieldType fieldType = ColumnType;

            // DateTimeEpochMs means DateTime field but stored as Epoch Ms but the column is DateTime.
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
            if (ColumnType == EFieldType.String
                || ColumnType == EFieldType.Symbol
                || ColumnType == EFieldType.Binary)
            {
                return typeof(IRefTypeColumn).GetMethod("SetValue");
            }
            EFieldType fieldType = ColumnType;
            // DateTimeEpochMs means DateTime field but stored as Epoch Ms but the column is DateTime.
            if (fieldType == EFieldType.DateTimeEpochMs)
            {
                fieldType = EFieldType.DateTime;
            }
            return typeof(IFixedWidthColumn).GetMethod("Set" + fieldType);
        }

        public bool IsRefType()
        {
            return DataType.IsClass;
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
            if (DataType.IsClass)
            {
                throw new NFSdbConfigurationException(
                    string.Format("Column of type '{0}' cannot be nullable", _columnDataType.ColumnTypeName));
            }
            var nullable = typeof (Nullable<>);
            return nullable.MakeGenericType(DataType);
        }
    }
}