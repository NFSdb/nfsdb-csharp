using System;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Collections;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Storage.Serializer;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core.Configuration
{
    public class JournalMetadata : IJournalMetadata
    {
        private readonly IList<ColumnMetadata> _columns;
        private readonly JournalSettings _settings;

        private readonly ExpandableList<SymbolCache> _symbolCaches = new ExpandableList<SymbolCache>(() => new SymbolCache());
        private IColumnStorage _symbolStorage;
        private readonly ISerializerFactory _serializerFactory;
        private readonly ColumnMetadata _timestampColumn;

        internal JournalMetadata(JournalElement config, ISerializerFactory serializerFactory, Type itemType)
        {
            _serializerFactory = serializerFactory;
            var columnSerializers = _serializerFactory.Initialize(itemType);
            columnSerializers = CheckColumnMatch(config, columnSerializers, itemType);
            _columns = ParseColumns(columnSerializers, config);
            _settings = new JournalSettings(config, _columns);

            // Parse.
            FileCount = CalcFilesCount(_columns);

            // Timestamp.
            if (!string.IsNullOrEmpty(config.TimestampColumn))
            {
                var timestampColumn = _columns.FirstOrDefault(c =>
                    c.PropertyName.Equals(config.TimestampColumn, StringComparison.OrdinalIgnoreCase));

                if (timestampColumn == null)
                {
                    throw new NFSdbConfigurationException("Timestamp column with name {0} is not found",
                        config.TimestampColumn);
                }

                if (timestampColumn.FieldType != EFieldType.DateTime
                    && timestampColumn.FieldType != EFieldType.DateTimeEpochMs
                    && timestampColumn.FieldType != EFieldType.Int64)
                {
                    throw new NFSdbConfigurationException("Timestamp column {0} must be DateTime or Int64 but was {1}",
                        config.TimestampColumn, timestampColumn.FieldType);
                }

                _timestampColumn = _columns.Single(c => c.FileName.Equals(config.TimestampColumn, StringComparison.OrdinalIgnoreCase));
                TimestampFieldID = _timestampColumn.FieldID;
            }


            // Misc.
            PartitionTtl = TimeSpan.FromMilliseconds(config.OpenPartitionTtl);
            Name = config.Class;
        }
        
        private int CalcFilesCount(IEnumerable<ColumnMetadata> columns)
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

        public string Name { get; private set; }

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

        private static IEnumerable<IColumnSerializerMetadata> CheckColumnMatch(JournalElement jconf,
            IEnumerable<IColumnSerializerMetadata> columns, Type itemType)
        {
            if (!jconf.FromDisk)
            {
                return columns;
            }

            var columnsList = columns as IList<IColumnSerializerMetadata>;
            if (columnsList == null) columnsList = columns.ToList();

            // Columns and column order must match.
            // If bitset exists it must be the last one.
            var hasBitSet = columnsList[columnsList.Count - 1].DataType == EFieldType.BitSet;
            var parsedColLen = hasBitSet ? columnsList.Count - 1 : columnsList.Count;
            var confColumns = jconf.Columns;
            if (parsedColLen != confColumns.Count)
            {
                throw new NFSdbConfigurationException("Settings loaded from disk has '{0}' " +
                                                      "columns but the serializer parsed " +
                                                      "class of '{1}' as '{2}' fields. Column count mismatch.",
                    confColumns.Count, itemType, parsedColLen);
            }


            for (int i = 0; i < confColumns.Count; i++)
            {
                bool found = false;
                for (int j = i; j < parsedColLen; j++)
                {
                    if (string.Equals(confColumns[i].Name, columnsList[j].PropertyName,
                        StringComparison.OrdinalIgnoreCase))
                    {
                        if (columnsList[j].DataType != confColumns[i].ColumnType)
                        {
                            if (!(confColumns[i].ColumnType == EFieldType.Symbol &&
                                  columnsList[i].DataType == EFieldType.String))
                            {
                                throw new NFSdbConfigurationException("Type '{0}' has field '{1}' of type '{2}' " +
                                                                      "while data on disk has column '{3}' of type '{4}'",
                                    itemType, columnsList[j].PropertyName, columnsList[j].DataType,
                                    confColumns[i].Name, confColumns[i].ColumnType);
                            }
                        }

                        // Put column j at place i in columnList
                        if (i != j)
                        {
                            var t = columnsList[j];
                            columnsList[j] = columnsList[i];
                            columnsList[i] = t;
                        }
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    throw new NFSdbConfigurationException("Type '{0}' does not have field for column name '{1}'",
                        itemType, confColumns[i].Name);
                }
            }
            return columnsList;
        }

        public Func<T, DateTime> GetTimestampReader<T>()
        {
            if (_timestampColumn == null)
            {
                return DummyTimestampDelegate;
            }
            if (_timestampColumn.FieldType == EFieldType.Int64)
            {
                var longReader = _serializerFactory.ColumnReader<T, long>(_timestampColumn.SerializerMetadata);
                return t => DateUtils.UnixTimestampToDateTime(longReader(t));
            }

            return _serializerFactory.ColumnReader<T, DateTime>(_timestampColumn.SerializerMetadata);
        }

        public static DateTime DummyTimestampDelegate<T>(T anyValue)
        {
            return DateTime.MinValue;
        }

        public ColumnMetadata GetColumnById(int columndID)
        {
            return _columns[columndID];
        }

        public ColumnMetadata GetColumnByPropertyName(string propertyName)
        {
            var col = _columns.FirstOrDefault(c => string.Equals(c.PropertyName, propertyName, StringComparison.OrdinalIgnoreCase));
            if (col == null)
            {
                throw new NFSdbConfigurationException("Property {0} does not exist in journal {1}",
                    propertyName, _settings.DefaultPath);
            }
            return col;
        }

        public int ColumnCount
        {
            get { return _columns.Count; }
        }

        public int GetColumnID(string filename)
        {
            int i = 0;
            foreach (var col in _columns)
            {
                if (string.Equals(filename, col.FileName, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
                i++;
            }
            return -1;
        }

        public int FileCount { get; private set; }
        public TimeSpan PartitionTtl { get; private set; }

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

                yield return new ColumnSource(cType.SerializerMetadata, column, fileID);
            }
        }

        private IList<ColumnMetadata> ParseColumns(IEnumerable<IColumnSerializerMetadata> columnsMetadata, JournalElement config)
        {
            // Build.
            var cols = new List<ColumnMetadata>();

            foreach (IColumnSerializerMetadata field in columnsMetadata)
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
                        var dateTimeConfig = config.Columns
                            .FirstOrDefault(c => c.Name.Equals(field.PropertyName, StringComparison.OrdinalIgnoreCase));

                        if (dateTimeConfig != null
                            && dateTimeConfig.ColumnType == EFieldType.DateTimeEpochMs)
                        {
                            field.DataType = EFieldType.DateTimeEpochMs;
                        }
                        cols.Add(ColumnMetadata.FromFixedField(field, cols.Count));
                        break;

                    case EFieldType.Symbol:
                    case EFieldType.String:
                        // Check config.
                        var stringConfig = (VarLenColumnElement) config.Columns
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
                        var binaryConfig = (VarLenColumnElement)config.Columns
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
    }
}