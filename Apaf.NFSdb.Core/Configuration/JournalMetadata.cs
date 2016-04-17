using System;
using System.Collections.Generic;
using System.Linq;
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
        private readonly ISerializerFactory _serializerFactory;
        private readonly ColumnMetadata _timestampColumn;

        internal JournalMetadata(JournalElement config, ISerializerFactory serializerFactory, Type itemType)
        {
            _serializerFactory = serializerFactory;
            var columnSerializers = _serializerFactory.Initialize(itemType);
            columnSerializers = CheckColumnMatch(config, columnSerializers, itemType);
            _columns = ParseColumns(columnSerializers, config.Columns);
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

                if (timestampColumn.ColumnType != EFieldType.DateTime
                    && timestampColumn.ColumnType != EFieldType.DateTimeEpochMs
                    && timestampColumn.ColumnType != EFieldType.Int64)
                {
                    throw new NFSdbConfigurationException("Timestamp column {0} must be DateTime or Int64 but was {1}",
                        config.TimestampColumn, timestampColumn.ColumnType);
                }

                _timestampColumn = _columns.Single(c => c.FileName.Equals(config.TimestampColumn, StringComparison.OrdinalIgnoreCase));
                TimestampColumnID = _timestampColumn.ColumnID;
            }

            // IsNull.
            var last = _columns.Last();
            if (last.ColumnType == EFieldType.BitSet)
            {
                IsNullColumnID = last.ColumnID;
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
                if (cType.ColumnType == EFieldType.String)
                {
                    fileID += 2;
                }
                else if (cType.ColumnType == EFieldType.BitSet)
                {
                    fileID++;
                }
                else if (cType.ColumnType == EFieldType.Symbol)
                {
                    fileID += 7;
                }
                if (cType.ColumnType == EFieldType.Binary)
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

        public IFieldSerializer GetSerializer(IEnumerable<ColumnSource> columns)
        {
            return _serializerFactory.CreateFieldSerializer(columns);
        }

        public IEnumerable<IColumnMetadata> Columns
        {
            get { return _columns; }
        }

        public JournalSettings Settings
        {
            get { return _settings; }
        }

        public int? TimestampColumnID { get; private set; }
        public int? IsNullColumnID { get; private set; }

        public ColumnSource[] GetPartitionColumns(int paritionID, IColumnStorage partitionStorage, PartitionConfig configOverride = null)
        {
            return CreateColumnsFromColumnMetadata(_columns, partitionStorage, configOverride, paritionID);
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
            var hasBitSet = columnsList[columnsList.Count - 1].ColumnType == EFieldType.BitSet;
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
                        if (columnsList[j].ColumnType != confColumns[i].ColumnType)
                        {
                            if (!(confColumns[i].ColumnType == EFieldType.Symbol &&
                                  columnsList[i].ColumnType == EFieldType.String))
                            {
                                throw new NFSdbConfigurationException("Type '{0}' has field '{1}' of type '{2}' " +
                                                                      "while data on disk has column '{3}' of type '{4}'",
                                    itemType, columnsList[j].PropertyName, columnsList[j].ColumnType,
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
            if (_timestampColumn.ColumnType == EFieldType.Int64)
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

        public IColumnMetadata GetColumnByID(int columndID)
        {
            return _columns[columndID];
        }

        public IColumnMetadata GetColumnByPropertyName(string propertyName)
        {
            var col = _columns.FirstOrDefault(c => string.Equals(c.PropertyName, propertyName, StringComparison.OrdinalIgnoreCase));
            if (col == null)
            {
                throw new NFSdbConfigurationException("Property {0} does not exist in journal {1}",
                    propertyName, _settings.DefaultPath);
            }
            return col;
        }

        public IColumnMetadata TryGetColumnByPropertyName(string propertyName)
        {
            var col = _columns.FirstOrDefault(c => string.Equals(c.PropertyName, propertyName, StringComparison.OrdinalIgnoreCase));
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

        private ColumnSource[] CreateColumnsFromColumnMetadata(IList<ColumnMetadata> columns, 
            IColumnStorage columnStorage, PartitionConfig configOverride, int partitionID)
        {
            var resultColumns = new ColumnSource[columns.Count];
            var recordHint = _settings.RecordHint;
            if (configOverride != null)
            {
                columns = ParseColumns(columns.Select(c => c.SerializerMetadata), configOverride.Columns);
                recordHint = configOverride.RecordHint;
            }

            int fileID = 0;
            for (int columnID = 0; columnID < columns.Count; columnID++)
            {
                var cType = columns[columnID];
                // Build.
                IColumn column;
                if (cType.ColumnType == EFieldType.String)
                {
                    // String.
                    var data = columnStorage.GetFile(cType, fileID++, EDataType.Data, recordHint);
                    var index = columnStorage.GetFile(cType, fileID++, EDataType.Index, recordHint);
                    column = new StringColumn(data, index, cType.MaxSize, GetPropertyName(cType.FileName));
                }
                else if (cType.ColumnType == EFieldType.BitSet)
                {
                    var data = columnStorage.GetFile(cType, fileID++, EDataType.Data, recordHint);
                    column = new BitsetColumn(data, cType.MaxSize);
                }
                else if (cType.ColumnType == EFieldType.Symbol)
                {
                    var colData = columnStorage.GetFile(cType, fileID++, EDataType.Data, recordHint);
                    var symData = columnStorage.GetFile(cType, fileID++, EDataType.Symd, recordHint);
                    var symi = columnStorage.GetFile(cType, fileID++, EDataType.Symi, recordHint);
                    var symk = columnStorage.GetFile(cType, fileID++, EDataType.Symrk, recordHint);
                    var symr = columnStorage.GetFile(cType, fileID++, EDataType.Symrr, recordHint);
                    int maxLen = cType.MaxSize;
                    int distinctHintCount = cType.HintDistinctCount;
                    if (cType.Indexed)
                    {
                        var colDataK = columnStorage.GetFile(cType, fileID++, EDataType.Datak, recordHint);
                        var colDataR = columnStorage.GetFile(cType, fileID++, EDataType.Datar, recordHint);
                        column = new SymbolMapColumn(
                            columnID,
                            partitionID,
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
                            maxLen: maxLen);
                    }
                    else
                    {
                        column = new SymbolMapColumn(
                            columnID,
                            partitionID,
                            data: colData,
                            symd: symData,
                            symi: symi,
                            symk: symk,
                            symr: symr,
                            propertyName: GetPropertyName(cType.FileName),
                            capacity: distinctHintCount,
                            maxLen: maxLen);
                    }
                }
                else if (cType.ColumnType == EFieldType.Binary)
                {
                    // Byte array.
                    var data = columnStorage.GetFile(cType, fileID++, EDataType.Data, recordHint);
                    var index = columnStorage.GetFile(cType, fileID++, EDataType.Index, recordHint);
                    column = new BinaryColumn(data, index, cType.MaxSize, GetPropertyName(cType.FileName));
                    
                }
                else
                {
                    // Fixed size.
                    var data = columnStorage.GetFile(cType, fileID++, EDataType.Data, recordHint);
                    column = new FixedColumn(data, cType.ColumnType, GetPropertyName(cType.FileName));
                }

                resultColumns[columnID] = new ColumnSource(cType, column, fileID);
            }
            return resultColumns;
        }

        private IList<ColumnMetadata> ParseColumns(IEnumerable<IColumnSerializerMetadata> columnsMetadata, List<ColumnElement> columns)
        {
            // Build.
            var cols = new List<ColumnMetadata>();
            int nullIndex = 0;

            foreach (IColumnSerializerMetadata field in columnsMetadata)
            {
                var nIndex = field.Nullable ? nullIndex++ : -1;
                // Type.
                switch (field.ColumnType)
                {
                    case EFieldType.Byte:
                    case EFieldType.Bool:
                    case EFieldType.Int16:
                    case EFieldType.Int32:
                    case EFieldType.Int64:
                    case EFieldType.Double:
                        cols.Add(ColumnMetadata.FromFixedField(field, cols.Count, nIndex));
                        break;

                    case EFieldType.DateTime:
                    case EFieldType.DateTimeEpochMs:
                        // Check config.
                        var dateTimeConfig = columns
                            .FirstOrDefault(c => c.Name.Equals(field.PropertyName, StringComparison.OrdinalIgnoreCase));

                        if (dateTimeConfig != null && dateTimeConfig.ColumnType == EFieldType.DateTimeEpochMs)
                        {
                            field.ColumnType = EFieldType.DateTimeEpochMs;
                        }
                        cols.Add(ColumnMetadata.FromFixedField(field, cols.Count, nIndex));
                        break;

                    case EFieldType.Symbol:
                    case EFieldType.String:
                        // Check config.
                        var stringConfig = (VarLenColumnElement) columns
                            .FirstOrDefault(c => c.Name.Equals(field.PropertyName,
                                StringComparison.OrdinalIgnoreCase));

                        if (stringConfig != null)
                        {
                            cols.Add(ColumnMetadata.FromColumnElement(field, stringConfig, cols.Count, nIndex));
                        }
                        else
                        {
                            // No config.
                            cols.Add(ColumnMetadata.FromStringField(field,
                                MetadataConstants.DEFAULT_STRING_AVG_SIZE,
                                MetadataConstants.DEFAULT_STRING_MAX_SIZE,
                                cols.Count, nIndex));
                        }
                        break;
                    case EFieldType.Binary:
                        var binaryConfig = (VarLenColumnElement)columns
                            .FirstOrDefault(c => c.Name.Equals(field.PropertyName,
                                StringComparison.OrdinalIgnoreCase));

                        if (binaryConfig != null)
                        {
                            cols.Add(ColumnMetadata.FromColumnElement(field, binaryConfig, cols.Count, nIndex));
                        }
                        else
                        {
                            // No config.
                            cols.Add(ColumnMetadata.FromBinaryField(field, 
                                MetadataConstants.DEFAULT_BINARY_AVG_SIZE,
                                MetadataConstants.DEFAULT_BINARY_MAX_SIZE,
                                cols.Count, nIndex));
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