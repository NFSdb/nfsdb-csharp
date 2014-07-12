#region copyright
/*
 * Copyright (c) 2014. APAF (Alex Pelagenko).
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
using Apaf.NFSdb.Core.Configuration;

namespace Apaf.NFSdb.Core.Column
{
    public class ColumnMetadata
    {
        private ColumnMetadata(SymbolElement symbolConfig, int fieldID) 
            : this((ColumnElement) symbolConfig, fieldID)
        {
            HintDistinctCount = symbolConfig.HintDistinctCount;
            Indexed = symbolConfig.Indexed;
            SameAs = symbolConfig.SameAs;
        }

        private ColumnMetadata(StringElement stringConfig, int fieldID)
            : this((ColumnElement)stringConfig, fieldID)
        {
        }

        private ColumnMetadata(ColumnElement configElement, int fieldID)
        {
            FieldID = fieldID;
            FieldType = configElement.ColumnType;
            AvgSize = GetStringAvgSize(configElement.AvgSize);
            MaxSize = configElement.MaxSize;
            FieldName = configElement.Name;
            PropertyName = GetPropertyName(configElement.Name);
        }

        private ColumnMetadata(EFieldType filedType, string fieldName, int avgSize, int maxSize, int fieldID)
        {
            FieldType = filedType;
            FieldName = fieldName;
            PropertyName = GetPropertyName(fieldName);
            AvgSize = avgSize;
            MaxSize = maxSize;
            FieldID = fieldID;
        }

        public int FieldID { get; private set; }
        public string SameAs { get; private set; }
        public bool Indexed { get; private set; }
        public EFieldType FieldType { get; private set; }
        public string FieldName { get; private set; }
        public string PropertyName { get; private set; }
        public int HintDistinctCount { get; private set; }
        public int AvgSize { get; private set; }
        public int MaxSize { get; private set; }

        public static ColumnMetadata FromColumnElement(ColumnElement colElement, int fieldID)
        {
            switch (colElement.ColumnType)
            {
                case EFieldType.String:
                    return new ColumnMetadata((StringElement)colElement, fieldID);
                case EFieldType.Symbol:
                    return new ColumnMetadata((SymbolElement)colElement, fieldID);
                default:
                    throw new ArgumentOutOfRangeException("colElement",
                        "ColumnType.ColumnType expected to be Symbol or String but was " +
                        colElement.ColumnType);
            }
        }

        public static ColumnMetadata FromStringField(string fieldName, int avgSize, int maxSize, int fieldID)
        {
            return new ColumnMetadata(EFieldType.String, fieldName, GetStringAvgSize(avgSize), maxSize, fieldID);
        }
        
        private static int GetStringAvgSize(int avgSize)
        {
            return 2*avgSize + StringHeaderSizeEstimate(avgSize);
        }

        private static int StringHeaderSizeEstimate(int avgSize)
        {
            if (avgSize < MetadataConstants.STRING_BYTE_LIMIT)
            {
                return 2;
            }
            if (avgSize < MetadataConstants.STRING_TWO_BYTE_LIMIT)
            {
                return 3;
            }
            return 5;
        }

        public static ColumnMetadata FromFixedField(EFieldType fieldType, string fieldName, int fieldID)
        {
            return new ColumnMetadata(fieldType, fieldName, fieldType.GetSize(), fieldType.GetSize(), fieldID);
        }

        public static ColumnMetadata FromBitsetField(string fieldName, int fieldCount, int fieldID)
        {
            return new ColumnMetadata(EFieldType.BitSet, fieldName, fieldCount, fieldCount, fieldID);
        }

        private static string GetPropertyName(string name)
        {
            return name.Substring(0, 1).ToUpper()
                   + name.Substring(1, name.Length - 1);
        }
    }
}