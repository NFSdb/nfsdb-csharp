using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Apaf.NFSdb.Core.Collections;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Reflection;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Configuration
{
    public class JournalMetadata<T> : IJournalMetadata<T>
    {
        // ReSharper disable once StaticFieldInGenericType
        private static readonly string ISSET_COLUMN_NAME = MetadataConstants.NULLS_FILE_NAME;
        private readonly IList<ColumnMetadata> _columns;
        private readonly JournalSettings _settings;

        private readonly ExpandableList<SymbolCache> _symbolCaches =
            new ExpandableList<SymbolCache>(() => new SymbolCache());

        private readonly Func<T, long> _timestampDelegate;
        private IColumnStorage _symbolStorage;

        public JournalMetadata(JournalElement config)
        {
            var itemType = typeof (T);

            // Parse.
            _columns = ParseColumns(itemType, config);
            FileCount = CalcFilesCount(_columns);

            // Timestamp.
            if (!string.IsNullOrEmpty(config.TimestampColumn))
            {
                _timestampDelegate = ReflectionHelper.CreateTimestampDelegate<T>(config.TimestampColumn);
                TimestampFieldID = _columns.Single(
                    c => c.FieldName.Equals(config.TimestampColumn,
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
                    _symbolCaches[columnMetadata.FieldID] = new SymbolCache();
                }
            }
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
        
        public IEnumerable<IColumn> GetPartitionColums(IColumnStorage partitionStorage)
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

        public Func<T, long> GetTimestampReader()
        {
            return _timestampDelegate;
        }

        public int GetFieldID(string arg)
        {
            int i = 0;
            foreach (var col in _columns)
            {
                if (string.Equals(arg, col.FieldName, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
                i++;
            }
            return -1;
        }

        public string KeySymbol { get; private set; }
        public int FileCount { get; private set; }

        private IEnumerable<IColumn> CreateColumnsFromColumnMetadata(IEnumerable<ColumnMetadata> columns,
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
                    var data = columnStorage.GetFile(cType.FieldName, fileID++, cType.FieldID, EDataType.Data);
                    var index = columnStorage.GetFile(cType.FieldName, fileID++, cType.FieldID, EDataType.Index);
                    column = new StringColumn(data, index, cType.MaxSize, GetPropertyName(cType.FieldName));
                }
                else if (cType.FieldType == EFieldType.BitSet)
                {
                    var data = columnStorage.GetFile(cType.FieldName, fileID++, cType.FieldID, EDataType.Data);
                    column = new BitsetColumn(data, _columns.Count);
                }
                else if (cType.FieldType == EFieldType.Symbol)
                {
                    var colData = columnStorage.GetFile(cType.FieldName, fileID++, cType.FieldID, EDataType.Data);
                    var colDataK = columnStorage.GetFile(cType.FieldName, fileID++, cType.FieldID, EDataType.Datak);
                    var colDataR = columnStorage.GetFile(cType.FieldName, fileID++, cType.FieldID, EDataType.Datar);
                    var symData = _symbolStorage.GetFile(cType.FieldName, fileID++, cType.FieldID, EDataType.Symd);
                    var symi = _symbolStorage.GetFile(cType.FieldName, fileID++, cType.FieldID, EDataType.Symi);
                    var symk = _symbolStorage.GetFile(cType.FieldName, fileID++, cType.FieldID, EDataType.Symrk);
                    var symr = _symbolStorage.GetFile(cType.FieldName, fileID++, cType.FieldID, EDataType.Symrr);
                    int maxLen = cType.MaxSize;
                    int distinctHintCount = cType.HintDistinctCount;
                    column = new SymbolMapColumn(
                        data: colData,
                        datak: colDataK,
                        datar: colDataR,
                        symd: symData,
                        symi: symi,
                        symk: symk,
                        symr: symr,
                        propertyName: GetPropertyName(cType.FieldName),
                        capacity: distinctHintCount,
                        recordCountHint: _settings.RecordHint,
                        maxLen: maxLen,
                        symbolCache: _symbolCaches[colData.FileID]);
                }
                else
                {
                    // Fixed size.
                    var data = columnStorage.GetFile(cType.FieldName, fileID++, cType.FieldID, EDataType.Data);
                    column = new FixedColumn(data, cType.FieldType, GetPropertyName(cType.FieldName));
                }

                yield return column;
            }
        }

        private IList<ColumnMetadata> ParseColumns(Type itemType, JournalElement config)
        {
            // Properties.
            // Public.
            IEnumerable<PropertyInfo> properties =
                itemType.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            // Build.
            var cols = new List<ColumnMetadata>();

            foreach (PropertyInfo property in properties)
            {
                var fieldName = GetFieldName(property.Name);

                // Type.
                if (property.PropertyType == typeof (byte))
                {
                    cols.Add(ColumnMetadata.FromFixedField(EFieldType.Byte, fieldName, cols.Count));
                }
                else if (property.PropertyType == typeof (bool))
                {
                    cols.Add(ColumnMetadata.FromFixedField(EFieldType.Bool, fieldName, cols.Count));
                }
                else if (property.PropertyType == typeof (short))
                {
                    cols.Add(ColumnMetadata.FromFixedField(EFieldType.Int16, fieldName, cols.Count));
                }
                else if (property.PropertyType == typeof (int))
                {
                    cols.Add(ColumnMetadata.FromFixedField(EFieldType.Int32, fieldName, cols.Count));
                }
                else if (property.PropertyType == typeof (long))
                {
                    cols.Add(ColumnMetadata.FromFixedField(EFieldType.Int64, fieldName, cols.Count));
                }
                else if (property.PropertyType == typeof (double))
                {
                    cols.Add(ColumnMetadata.FromFixedField(EFieldType.Double, fieldName, cols.Count));
                }
                else if (property.PropertyType == typeof (string))
                {
                    // Check config.
                    var stringConfig = ((IEnumerable<ColumnElement>) (config.Strings))
                        .Concat(config.Symbols)
                        .FirstOrDefault(c => c.Name.Equals(fieldName,
                            StringComparison.OrdinalIgnoreCase));

                    if (stringConfig != null)
                    {
                        cols.Add(ColumnMetadata.FromColumnElement(stringConfig, cols.Count));
                    }
                    else
                    {
                        // No config.
                        cols.Add(ColumnMetadata.FromStringField(fieldName,
                            MetadataConstants.DEFAULT_STRING_AVG_SIZE,
                            MetadataConstants.DEFAULT_STRING_MAX_SIZE,
                            cols.Count));
                    }
                }
                else
                {
                    throw new NFSdbConfigurationException("Unsupported property type " + property.PropertyType);
                }
            }
            var issetField = itemType.GetField(MetadataConstants.ISSET_FIELD_NAME);
            if (issetField.FieldType.Name.EndsWith("Isset"))
            {
                var fieldSize = BitsetColumn.CalculateSize(cols.Count);
                cols.Add(ColumnMetadata.FromBitsetField(ISSET_COLUMN_NAME, fieldSize, cols.Count));
            }
            return cols;
        }

        private static string GetFieldName(string properyName)
        {
            return properyName.Substring(0, 1).ToLower()
                   + properyName.Substring(1, properyName.Length - 1);
        }

        private static string GetPropertyName(string name)
        {
            return name.Substring(0, 1).ToUpper()
                   + name.Substring(1, name.Length - 1);
        }

        private static long DefaultGetTimestamp(T item)
        {
            return 0L;
        }
    }
}