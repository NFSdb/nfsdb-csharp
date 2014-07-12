using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Apaf.NJournal.Core.Collections;
using Apaf.NJournal.Core.Column;
using Apaf.NJournal.Core.Exceptions;
using Apaf.NJournal.Core.Reflection;
using Apaf.NJournal.Core.Storage;

namespace Apaf.NJournal.Core
{
    public class JournalMetadata : IJournalMetadata
    {
        private readonly IArray<ColumnMetadata> _columns;
        private readonly Func<object> _constructor;
        private readonly Type _itemType;
        private readonly JournalSettings _settings;
        private static readonly int DEFAULT_STRING_MAX_LEN = MetadataConstants.STRING_BYTE_LIMIT;
        private static readonly string ISSET_COLUMN_NAME = MetadataConstants.ISSET_COLUMN_NAME;

        public JournalMetadata(Type itemType, JournalSettings settings,
            IArray<ColumnStaticMetadata> staticMetadata, string[] privateFields = null)
        {
            _itemType = itemType;
            _settings = settings;

            // Constructor delegate.
            _constructor = ReflectionHelper.CreateConstructorDelegate(_itemType);

            // Parse.
            _columns = ParseColumns(itemType, staticMetadata, privateFields);
        }

        public IEnumerable<ColumnMetadata> Columns
        {
            get { return _columns; }
        }

        public object NewObject()
        {
            return _constructor();
        }

        public IFieldReader GetObjectReader(IColumnStorage columnStorage)
        {
            var columns = CreateColumnsFromColumnMetadata(_columns, columnStorage);
            return new ReflectionObjectReader(_itemType, columns, _constructor);
        }

        private IEnumerable<IColumn> CreateColumnsFromColumnMetadata(IArray<ColumnMetadata> columns, 
            IColumnStorage columnStorage)
        {
            foreach (var cType in columns)
            {
                // Build.
                IColumn column;
                if (cType.FieldType == EFieldType.String)
                {
                    // String.
                    var data = columnStorage.GetFile(cType.FieldName, EDataType.Data);
                    var index = columnStorage.GetFile(cType.FieldName, EDataType.Index);
                    int maxLen = cType.MaxLength ?? DEFAULT_STRING_MAX_LEN;
                    column = new StringColumn(data, index, maxLen, GetPropertyName(cType.FieldName));
                }
                else if (cType.FieldType == EFieldType.BitSet)
                {
                    var data = columnStorage.GetFile(cType.FieldName, EDataType.Data);
                    column = new BitsetColumn(data, columns);
                }
                else
                {
                    // Fixed size.
                    var data = columnStorage.GetFile(cType.FieldName, EDataType.Data);
                    column = new FixedColumn(data, cType.FieldType, GetPropertyName(cType.FieldName));
                }

                yield return column;
            }
        }

        private static IArray<ColumnMetadata> ParseColumns(
            Type itemType, IArray<ColumnStaticMetadata> staticMetadata, IEnumerable<string> allowedPrivate)
        {
            // Properties.
            // Public.
            IEnumerable<PropertyInfo> properties =
                itemType.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            // Private.
            if (allowedPrivate != null)
            {
                var allowed = new HashSet<string>(allowedPrivate, StringComparer.OrdinalIgnoreCase);

                var privateProperties =
                    itemType.GetProperties(BindingFlags.Instance | BindingFlags.NonPublic);

                properties =
                    properties.Concat(privateProperties.Where(p => allowed.Contains(p.Name)));
            }

            // Build.
            var cols = new List<ColumnMetadata>();

            foreach (PropertyInfo property in properties)
            {
                var fieldName = GetFieldName(property.Name);
                var staticMeta = staticMetadata
                     .FirstOrDefault(c => c.Name.Equals(fieldName,
                         StringComparison.OrdinalIgnoreCase));

                // Type.
                if (property.PropertyType == typeof (byte))
                {
                    cols.Add(new ColumnMetadata(EFieldType.Byte, fieldName));
                }
                else if (property.PropertyType == typeof (bool))
                {
                    cols.Add(new ColumnMetadata(EFieldType.Bool, fieldName));
                }
                else if (property.PropertyType == typeof (short))
                {
                    cols.Add(new ColumnMetadata(EFieldType.Int16, fieldName));
                }
                else if (property.PropertyType == typeof (int))
                {
                    cols.Add(new ColumnMetadata(EFieldType.Int32, fieldName));
                }
                else if (property.PropertyType == typeof (long))
                {
                    cols.Add(new ColumnMetadata(EFieldType.Int64, fieldName));
                }
                else if (property.PropertyType == typeof (double))
                {
                    cols.Add(new ColumnMetadata(EFieldType.Double, fieldName));
                }
                else if (property.PropertyType == typeof (string))
                {
                    var maxLen = staticMeta != null ? staticMeta.MaxLength : (int?)null;
                    cols.Add(new ColumnMetadata(EFieldType.String, fieldName, maxLen));
                }
                else
                {
                    throw new NJournalConfigurationException("Unsupported property type " + property.PropertyType);
                }
            }
            var issetField = itemType.GetField(MetadataConstants.ISSET_FIELD_NAME);
            if (issetField.FieldType.Name.EndsWith("Isset"))
            {
                cols.Add(new ColumnMetadata(EFieldType.BitSet, ISSET_COLUMN_NAME));
            }
            return new ArrayWrapper<ColumnMetadata>(cols);
        }


        //private static List<IColumn> GetColumns(IArray<ColumnMetadata> columnTypes,
        //    JournalSettings settings)
        //{
        //    var columns = new List<IColumn>(columnTypes.Count);
        //    foreach (var cType in columnTypes)
        //    {
        //        // Check.
        //        if (cType.FieldType == EFieldType.BitSet)
        //        {
        //            throw new ArgumentException("Internal error. Unexpected column type " + cType);
        //        }

        //        // Build.
        //        IColumn column;
        //        if (cType.FieldType == EFieldType.String)
        //        {
        //            // String.
        //            var data = GetStorage(cType, settings);
        //            var index = GetIndexStorage(cType, settings);
        //            int maxLen = cType.MaxLength ?? DEFAULT_STRING_MAX_LEN;
        //            column = new StringColumn(data, index, maxLen, GetPropertyName(cType.FieldName));
        //        }
        //        else
        //        {
        //            // Fixed size.
        //            var data = GetStorage(cType, settings);
        //            column = new FixedColumn(data, cType.FieldType, GetPropertyName(cType.FieldName));
        //        }

        //        // Add.
        //        columns.Add(column);
        //    }
        //    return columns;
        //}

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
    }
}