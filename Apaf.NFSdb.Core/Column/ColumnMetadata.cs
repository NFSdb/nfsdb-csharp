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
using System.Collections.Generic;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage.Serializer;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    internal class ColumnMetadata : IColumnMetadata
    {
        private ColumnMetadata(IColumnSerializerMetadata serializerMetadata,
            SymbolElement symbolConfig, int columnID, int nullIndex)
            : this(serializerMetadata, (VarLenColumnElement) symbolConfig, columnID, nullIndex)
        {
            HintDistinctCount = symbolConfig.HintDistinctCount;
            Indexed = symbolConfig.Indexed;
            SameAs = symbolConfig.SameAs;
        }

        private ColumnMetadata(IColumnSerializerMetadata serializerMetadata, 
            VarLenColumnElement configElement, int columnID, int nullIndex)
            :this(serializerMetadata, columnID, nullIndex)
        {
            MaxSize = configElement.MaxSize ?? (configElement.AvgSize ?? MetadataConstants.DEFAULT_STRING_MAX_SIZE);
            AvgSize = GetStringAvgSize(configElement.AvgSize ?? (configElement.MaxSize ?? MetadataConstants.DEFAULT_SYMBOL_AVG_SIZE));
        }


        private ColumnMetadata(IColumnSerializerMetadata serializerMetadata,
            int avgSize, int maxSize, int columnID, int nullIndex)
            : this(serializerMetadata, columnID, nullIndex)
        {
            AvgSize = avgSize;
            MaxSize = maxSize;
        }

        private ColumnMetadata(IColumnSerializerMetadata serializerMetadata,
            int columnID, int nullIndex)
        {
            SerializerMetadata = serializerMetadata;
            ColumnID = columnID;
            Nullable = serializerMetadata.Nullable;
            NullIndex = nullIndex;
            DataType = JournalColumnRegistry.Instance.GetSerializer(serializerMetadata.ColumnType);
        }

        public int NullIndex { get; private set; }
        public int ColumnID { get; private set; }
        public string SameAs { get; private set; }
        public bool Indexed { get; private set; }
        public bool Nullable { get; private set; }
        public IColumnDataType DataType { get; private set; }
        public IColumnSerializerMetadata SerializerMetadata { get; private set; }

        public string FileName
        {
            get { return SerializerMetadata.GetFileName(); }
        }

        public string PropertyName
        {
            get { return SerializerMetadata.PropertyName; }
        }

        public int HintDistinctCount { get; private set; }

        public IComparer<long> GetColumnComparer(IReadTransactionContext tx, bool asc)
        {
            return DataType.GetColumnComparer(ColumnID, tx, asc);
        }

        public int AvgSize { get; private set; }
        public int MaxSize { get; private set; }
        public EFieldType ColumnType { get { return DataType.ColumnType; } }

        public static ColumnMetadata FromColumnElement(IColumnSerializerMetadata metadata, 
            VarLenColumnElement colElement, int fieldID, int nullIndex)
        {
            switch (colElement.ColumnType)
            {
                case EFieldType.String:
                    return new ColumnMetadata(metadata, colElement, fieldID, nullIndex);
                case EFieldType.Symbol:
                    metadata.ColumnType = EFieldType.Symbol;
                    return new ColumnMetadata(metadata, (SymbolElement)colElement, fieldID, nullIndex);
                default:
                    throw new ArgumentOutOfRangeException("colElement",
                        "ColumnType.ColumnType expected to be Symbol or String but was " +
                        colElement.ColumnType);
            }
        }

        public static ColumnMetadata FromStringField(IColumnSerializerMetadata serializerMetadata,
            int avgSize, int maxSize, int fieldID, int nullIndex)
        {
            return new ColumnMetadata(serializerMetadata, GetStringAvgSize(avgSize), maxSize, fieldID, nullIndex);
        }

        public static ColumnMetadata FromBinaryField(IColumnSerializerMetadata serializerMetadata,
            int avgSize, int maxSize, int fieldID, int nullIndex)
        {
            return new ColumnMetadata(serializerMetadata, GetBinaryAvgSize(avgSize), maxSize, fieldID, nullIndex);
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

        public static ColumnMetadata FromFixedField(IColumnSerializerMetadata serializerMetadata, 
            int fieldID, int nullIndex)
        {
            return new ColumnMetadata(serializerMetadata, 
                serializerMetadata.ColumnType.GetSize(), 
                serializerMetadata.ColumnType.GetSize(),
                fieldID, nullIndex);
        }

        public static ColumnMetadata FromBitsetField(IColumnSerializerMetadata serializerMetadata,
            int fieldCount, int fieldID)
        {
            return new ColumnMetadata(serializerMetadata, fieldCount, fieldCount, fieldID, -1);
        }

        public int GetConfigAvgSize()
        {
            return (AvgSize - VarBinaryHeaderSizeEstimate())/2;
        }

        public object ToTypedValue(object literal)
        {
            return DataType.ToTypedValue(literal);
        }
    }
}