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

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class RecordSerializerMetadata : IColumnSerializerMetadata
    {
        public RecordSerializerMetadata(EFieldType type, string propertyName, 
            bool nullable, int columnId, int size = 0)
        {
            ColumnType = type;
            PropertyName = propertyName;
            Size = size;
            Nullable = nullable;
            ColumnId = columnId;
        }

        public EFieldType ColumnType { get; set; }
        public Type DataType { get; set; }
        public int Size { get; private set; }
        public bool Nullable { get; private set; }
        public int ColumnId { get; set; }
        public string PropertyName { get; private set; }

        public string GetFileName()
        {
            return PropertyName.Substring(0, 1).ToLower() + PropertyName.Substring(1);
        }
    }
}