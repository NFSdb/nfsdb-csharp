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
using Apaf.NFSdb.Core.Configuration;

namespace Apaf.NFSdb.Core.Column
{
    public class ColumnMetadata
    {
        private ColumnMetadata(IColumnSerializerMetadata serializerMetadata, 
            SymbolElement symbolConfig, int fieldID)
            : this(serializerMetadata, (ColumnElement) symbolConfig, fieldID)
        {
            HintDistinctCount = symbolConfig.HintDistinctCount;
            Indexed = symbolConfig.Indexed;
            SameAs = symbolConfig.SameAs;
        }

        private ColumnMetadata(IColumnSerializerMetadata serializerMetadata, 
            StringElement stringConfig, int fieldID)
            : this(serializerMetadata, (ColumnElement) stringConfig, fieldID)
        {
        }

        private ColumnMetadata(IColumnSerializerMetadata serializerMetadata, 
            ColumnElement configElement, int fieldID)
        {
            SerializerMetadata = serializerMetadata;
            FieldID = fieldID;
            AvgSize = GetStringAvgSize(configElement.AvgSize);
            MaxSize = configElement.MaxSize;
        }

        private ColumnMetadata(IColumnSerializerMetadata serializerMetadata,
            int avgSize, int maxSize, int fieldID)
        {
            SerializerMetadata = serializerMetadata;
            AvgSize = avgSize;
            MaxSize = maxSize;
            FieldID = fieldID;
        }

        public int FieldID { get; private set; }
        public string SameAs { get; private set; }
        public bool Indexed { get; private set; }

        public EFieldType FieldType
        {
            get { return SerializerMetadata.DataType; }
        }

        public string FileName
        {
            get { return SerializerMetadata.GetFieldName(); }
        }

        public string PropertyName
        {
            get { return SerializerMetadata.PropertyName; }
        }

        public int HintDistinctCount { get; private set; }
        public int AvgSize { get; private set; }
        public int MaxSize { get; private set; }
        public IColumnSerializerMetadata SerializerMetadata { get; private set; }

        public static ColumnMetadata FromColumnElement(IColumnSerializerMetadata metadata, 
            ColumnElement colElement, int fieldID)
        {
            switch (colElement.ColumnType)
            {
                case EFieldType.String:
                    return new ColumnMetadata(metadata, (StringElement) colElement, fieldID);
                case EFieldType.Symbol:
                    var symbolMeta = new ColumnSerializerMetadata(EFieldType.Symbol, metadata.PropertyName,
                        metadata.Nulllable, metadata.Size);
                    return new ColumnMetadata(symbolMeta, (SymbolElement) colElement, fieldID);
                default:
                    throw new ArgumentOutOfRangeException("colElement",
                        "ColumnType.ColumnType expected to be Symbol or String but was " +
                        colElement.ColumnType);
            }
        }

        public static ColumnMetadata FromStringField(IColumnSerializerMetadata serializerMetadata, 
            int avgSize, int maxSize, int fieldID)
        {
            return new ColumnMetadata(serializerMetadata, GetStringAvgSize(avgSize), maxSize, fieldID);
        }

        public static ColumnMetadata FromBinaryField(IColumnSerializerMetadata serializerMetadata,
            int avgSize, int maxSize, int fieldID)
        {
            return new ColumnMetadata(serializerMetadata, GetBinaryAvgSize(avgSize), maxSize, fieldID);
        }

        private static int GetStringAvgSize(int avgSize)
        {
            return 2*avgSize + VarBinaryHeaderSizeEstimate();
        }

        private static int GetBinaryAvgSize(int avgSize)
        {
            return 2*avgSize + VarBinaryHeaderSizeEstimate();
        }

        private static int VarBinaryHeaderSizeEstimate()
        {
            return MetadataConstants.LARGE_VAR_COL_HEADER_LENGTH;
        }

        public static ColumnMetadata FromFixedField(IColumnSerializerMetadata serializerMetadata, int fieldID)
        {
            return new ColumnMetadata(serializerMetadata, 
                serializerMetadata.DataType.GetSize(), 
                serializerMetadata.DataType.GetSize(), 
                fieldID);
        }

        public static ColumnMetadata FromBitsetField(IColumnSerializerMetadata serializerMetadata, 
            int fieldCount, int fieldID)
        {
            return new ColumnMetadata(serializerMetadata, fieldCount, fieldCount, fieldID);
        }
    }
}