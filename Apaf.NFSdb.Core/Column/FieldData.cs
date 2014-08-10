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

using System.Reflection;

namespace Apaf.NFSdb.Core.Column
{
    public class FieldData
    {
        public FieldData(EFieldType type, string propertyName, bool nullable = false, int size = 0)
        {
            DataType = type;
            PropertyName = propertyName;
            Size = size;
            Nulllable = nullable;
        }

        public EFieldType DataType { get; private set; }
        public string PropertyName { get; private set; }
        public int Size { get; private set; }
        public bool Nulllable { get; private set; }

        public MethodInfo GetGetMethod()
        {
            if (DataType == EFieldType.String
                || DataType == EFieldType.Symbol)
            {
                return typeof(IStringColumn).GetMethod("GetString");
            }
            EFieldType fieldType = DataType;
            return typeof(IFixedWidthColumn).GetMethod("Get" + fieldType);
        }

        public string GetFieldName()
        {
            return PropertyName.Substring(0, 1).ToLower() + PropertyName.Substring(1);
        }

        public MethodInfo GetSetMethod()
        {
            if (DataType == EFieldType.String
                || DataType == EFieldType.Symbol)
            {
                return typeof(IStringColumn).GetMethod("SetString");
            }
            EFieldType fieldType = DataType;
            return typeof(IFixedWidthColumn).GetMethod("Set" + fieldType);
        }
    }
}