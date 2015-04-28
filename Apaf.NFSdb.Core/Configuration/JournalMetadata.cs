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
using System.Linq;
using Apaf.NFSdb.Core.Collections;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Reflection;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Storage.Serializer;

namespace Apaf.NFSdb.Core.Configuration
{
    public class JournalMetadata<T> : IJournalMetadata<T>
    {
        private readonly IList<ColumnMetadata> _columns;
        private readonly JournalSettings _settings;

        private readonly ExpandableList<SymbolCache> _symbolCaches =
            new ExpandableList<SymbolCache>(() => new SymbolCache());

        private readonly Func<T, DateTime> _timestampDelegate;
        private IColumnStorage _symbolStorage;
        private readonly ISerializerFactory _serializerFactory;

        public JournalMetadata(JournalElement config)
        {
            var itemType = typeof (T);

            _serializerFactory =
                JournalSerializers.Instance.GetSerializer(config.SerializerName ??
                                                          MetadataConstants.DEFAULT_SERIALIZER_NAME);
            _serializerFactory.Initialize(itemType);
            var columnsFields = _serializerFactory.ParseColumns().ToArray();
            _columns = ParseColumns(columnsFields, config);

            // Parse.
            FileCount = CalcFilesCount(_columns);

            // Timestamp.
            if (!string.IsNullOrEmpty(config.TimestampColumn))
            {
                var timestampColumn = columnsFields.FirstOrDefault(c => 
                    c.PropertyName.Equals(config.TimestampColumn, StringComparison.OrdinalIgnoreCase));

                if (timestampColumn == null)
                {
                    throw new NFSdbConfigurationException("Timestamp column with name {0} is not found", 
                        config.TimestampColumn);
                }

                if (timestampColumn.DataType != EFieldType.DateTime
                    && timestampColumn.DataType != EFieldType.DateTimeEpochMilliseconds
                    && timestampColumn.DataType != EFieldType.Int64)
                {
                    throw new NFSdbConfigurationException("Timestamp column {0} must be DateTime or Int64 but was {1}",
                        config.TimestampColumn, timestampColumn.DataType);
                }
                _timestampDelegate = 
                    ReflectionHelper.CreateTimestampDelegate<T>(timestampColumn.FieldName);

                TimestampFieldID = _columns.Single(
                    c => c.FileName.Equals(config.TimestampColumn,
                        StringComparison.OrdinalIgnoreCase)).FieldID;
            }
            else
            {
                _timestampDelegate = DefaultGetTimestamp;
            }

            // Create settings.
            _settings = new JournalSettings(config, _columns);

            // Misc.
            KeySymbol = config.Key != null ? GetPropertyName(config.Key) : null;
        }

        private int CalcFilesCount(IList<ColumnMetadata> columns)
        {
            int fileID = 0;
            foreach (var cType in columns)
            {
                if (cType.FieldType == EFieldType.String)
                {
                    fileID += 2;
                }
                else if (cType.FieldType == EFieldType.BitSet)
                {
                    fileID++;
                }
                else if (cType.FieldType == EFieldType.Symbol)
                {
                    fileID += 7;
                }
                if (cType.FieldType == EFieldType.Binary)
                {
                    fileID += 2;
                }
                else
                {
                    fileID++;
                }
            }
            return fileID;
        }

        public void InitializeSymbols(IColumnStorage symbolStorage)
        {
            if (_symbolStorage != null)
            {
                throw new NFSdbInitializationException("Symbols are alrady initialized");
            }
            // Create symbols.
            _symbolStorage = symbolStorage;
            foreach (var columnMetadata in _columns)
            {
                if (columnMetadata.FieldType == EFieldType.Symbol)
                {
                    _symbolCaches[columnMetadata.FieldID] = new SymbolCache(
                        columnMetadata.HintDistinctCount);
                }
            }
        }

        public IFieldSerializer GetSerializer(IEnumerable<ColumnSource> columns)
        {
            return _serializerFactory.CreateFieldSerializer(columns);
        }

        public IEnumerable<ColumnMetadata> Columns
        {
            get { return _columns; }
        }

        public JournalSettings Settings
        {
            get { return _settings; }
        }

        public int? TimestampFieldID { get; private set; }

        public IEnumerable<ColumnSource> GetPartitionColums(IColumnStorage partitionStorage)
        {
            if (_symbolStorage == null)
            {
                throw new NFSdbInitializationException(
                    "Symbols are not initialized. Please call InitializeSymbols first");
            }
            return CreateColumnsFromColumnMetadata(_columns, partitionStorage);
        }

        public ColumnMetadata GetColumnById(int columndID)
        {
            return _columns[columndID];
        }

        public Func<T, DateTime> GetTimestampReader()
        {
            return _timestampDelegate;
        }

        public int GetFieldID(string arg)
        {
            int i = 0;
            foreach (var col in _columns)
            {
                if (string.Equals(arg, col.FileName, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
                i++;
            }
            return -1;
        }

        public string KeySymbol { get; private set; }
        public int FileCount { get; private set; }

        private IEnumerable<ColumnSource> CreateColumnsFromColumnMetadata(IEnumerable<ColumnMetadata> columns,
            IColumnStorage columnStorage)
        {
            int fileID = 0;
            foreach (var cType in columns)
            {
                // Build.
                IColumn column;
                if (cType.FieldType == EFieldType.String)
                {
                    // String.
                    var data = columnStorage.GetFile(cType.FileName, fileID++, cType.FieldID, EDataType.Data);
                    var index = columnStorage.GetFile(cType.FileName, fileID++, cType.FieldID, EDataType.Index);
                    column = new StringColumn(data, index, cType.MaxSize, GetPropertyName(cType.FileName));
                }
                else if (cType.FieldType == EFieldType.BitSet)
                {
                    var data = columnStorage.GetFile(cType.FileName, fileID++, cType.FieldID, EDataType.Data);
                    column = new BitsetColumn(data, cType.MaxSize);
                }
                else if (cType.FieldType == EFieldType.Symbol)
                {
                    var colData = columnStorage.GetFile(cType.FileName, fileID++, cType.FieldID, EDataType.Data);
                    var symData = _symbolStorage.GetFile(cType.FileName, fileID++, cType.FieldID, EDataType.Symd);
                    var symi = _symbolStorage.GetFile(cType.FileName, fileID++, cType.FieldID, EDataType.Symi);
                    var symk = _symbolStorage.GetFile(cType.FileName, fileID++, cType.FieldID, EDataType.Symrk);
                    var symr = _symbolStorage.GetFile(cType.FileName, fileID++, cType.FieldID, EDataType.Symrr);
                    int maxLen = cType.MaxSize;
                    int distinctHintCount = cType.HintDistinctCount;
                    if (cType.Indexed)
                    {
                        var colDataK = columnStorage.GetFile(cType.FileName, fileID++, cType.FieldID, EDataType.Datak);
                        var colDataR = columnStorage.GetFile(cType.FileName, fileID++, cType.FieldID, EDataType.Datar);
                        column = new SymbolMapColumn(
                            data: colData,
                            datak: colDataK,
                            datar: colDataR,
                            symd: symData,
                            symi: symi,
                            symk: symk,
                            symr: symr,
                            propertyName: GetPropertyName(cType.FileName),
                            capacity: distinctHintCount,
                            recordCountHint: _settings.RecordHint,
                            maxLen: maxLen,
                            symbolCache: _symbolCaches[colData.FileID]);
                    }
                    else
                    {
                        column = new SymbolMapColumn(
                            data: colData,
                            symd: symData,
                            symi: symi,
                            symk: symk,
                            symr: symr,
                            propertyName: GetPropertyName(cType.FileName),
                            capacity: distinctHintCount,
                            maxLen: maxLen,
                            symbolCache: _symbolCaches[colData.FileID]);
                    }
                }
                else if (cType.FieldType == EFieldType.Binary)
                {
                    // Byte array.
                    var data = columnStorage.GetFile(cType.FileName, fileID++, cType.FieldID, EDataType.Data);
                    var index = columnStorage.GetFile(cType.FileName, fileID++, cType.FieldID, EDataType.Index);
                    column = new BinaryColumn(data, index, cType.MaxSize, GetPropertyName(cType.FileName));
                    
                }
                else
                {
                    // Fixed size.
                    var data = columnStorage.GetFile(cType.FileName, fileID++, cType.FieldID, EDataType.Data);
                    column = new FixedColumn(data, cType.FieldType, GetPropertyName(cType.FileName));
                }

                yield return new ColumnSource(cType.SerializerMetadata, column);
            }
        }

        private IList<ColumnMetadata> ParseColumns(IEnumerable<IColumnSerializerMetadata> fields, JournalElement config)
        {
            // Build.
            var cols = new List<ColumnMetadata>();

            foreach (IColumnSerializerMetadata field in fields)
            {
                // Type.
                switch (field.DataType)
                {
                    case EFieldType.Byte:
                    case EFieldType.Bool:
                    case EFieldType.Int16:
                    case EFieldType.Int32:
                    case EFieldType.Int64:
                    case EFieldType.Double:
                        cols.Add(ColumnMetadata.FromFixedField(field, cols.Count));
                        break;

                    case EFieldType.DateTime:
                        // Check config.
                        var dateTimeConfig = ((IEnumerable<ColumnElement>)(config.DateTimes))
                            .Concat(config.Symbols)
                            .FirstOrDefault(c => c.Name.Equals(field.PropertyName,
                                StringComparison.OrdinalIgnoreCase));

                        if (dateTimeConfig != null
                            && ((DateTimeElement)dateTimeConfig).IsEpochMilliseconds)
                        {
                            cols.Add(ColumnMetadata.FromFixedField(
                                new ColumnSerializerMetadata(EFieldType.DateTimeEpochMilliseconds, 
                                    field.PropertyName, field.FieldName, field.Nulllable), 
                                cols.Count));
                        }
                        else
                        {
                            cols.Add(ColumnMetadata.FromFixedField(field, cols.Count));
                        }
                        break;

                    case EFieldType.Symbol:
                    case EFieldType.String:
                        // Check config.
                        var stringConfig = ((IEnumerable<ColumnElement>) (config.Strings))
                            .Concat(config.Symbols)
                            .FirstOrDefault(c => c.Name.Equals(field.PropertyName,
                                StringComparison.OrdinalIgnoreCase));

                        if (stringConfig != null)
                        {
                            cols.Add(ColumnMetadata.FromColumnElement(field, stringConfig, cols.Count));
                        }
                        else
                        {
                            // No config.
                            cols.Add(ColumnMetadata.FromStringField(field,
                                MetadataConstants.DEFAULT_STRING_AVG_SIZE,
                                MetadataConstants.DEFAULT_STRING_MAX_SIZE,
                                cols.Count));
                        }
                        break;
                    case EFieldType.Binary:
                        var binaryConfig = ((IEnumerable<ColumnElement>) (config.Binaries))
                            .FirstOrDefault(c => c.Name.Equals(field.PropertyName,
                                StringComparison.OrdinalIgnoreCase));

                        if (binaryConfig != null)
                        {
                            cols.Add(ColumnMetadata.FromColumnElement(field, binaryConfig, cols.Count));
                        }
                        else
                        {
                            // No config.
                            cols.Add(ColumnMetadata.FromBinaryField(field, 
                                MetadataConstants.DEFAULT_BINARY_AVG_SIZE,
                                MetadataConstants.DEFAULT_BINARY_MAX_SIZE,
                                cols.Count));
                        }
                        break;
                    case EFieldType.BitSet:
                        var fieldSize = BitsetColumn.CalculateSize(field.Size);
                        cols.Add(ColumnMetadata.FromBitsetField(field, fieldSize, cols.Count));
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            return cols;
        }
        
        private static string GetPropertyName(string name)
        {
            return name.Substring(0, 1).ToUpper()
                   + name.Substring(1, name.Length - 1);
        }

        private static DateTime DefaultGetTimestamp(T item)
        {
            return DateTime.MinValue;
        }
    }
}