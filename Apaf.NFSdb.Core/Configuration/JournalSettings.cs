using System;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Configuration
{
    public class JournalSettings
    {
        private readonly ColumnMetadata[] _columns;
        private readonly string _defaultPath;
        private readonly string _keySymbol;
        private readonly int _lagHours;
        private readonly int _maxOpenPartitions;
        private readonly int _openPartitionTTL;
        private EPartitionType _partitionType;
        private readonly int _recordHint;
        private readonly string _timestampColumn;
        private const int INT32_SIZE = 4;

        public JournalSettings(JournalElement jconf, IEnumerable<ColumnMetadata> actualColumns)
        {
            _defaultPath = jconf.DefaultPath;
            _timestampColumn = jconf.TimestampColumn;
            _keySymbol = jconf.Key;
            _partitionType = jconf.PartitionType;
            _openPartitionTTL = jconf.OpenPartitionTTL;
            _maxOpenPartitions = jconf.MaxOpenPartitions;
            _lagHours = jconf.LagHours;
            _columns = actualColumns.ToArray();
            Columns = _columns;

            _recordHint = jconf.RecordHint;
            if (_recordHint < 0) _recordHint = MetadataConstants.DEFAULT_RECORD_HINT;

        }

        public IEnumerable<ColumnMetadata> Columns { get; private set; }

        public string DefaultPath
        {
            get { return _defaultPath; }
        }

        public string TimestampColumn
        {
            get { return _timestampColumn; }
        }

        public string KeySymbol
        {
            get { return _keySymbol; }
        }

        public EPartitionType PartitionType
        {
            get { return _partitionType; }
        }

        public int RecordHint
        {
            get { return _recordHint; }
        }

        public int OpenPartitionTTL
        {
            get { return _openPartitionTTL; }
        }

        public int MaxOpenPartitions
        {
            get { return _maxOpenPartitions; }
        }

        public int LagHours
        {
            get { return _lagHours; }
        }

        public ColumnMetadata GetColumn(string fieldName)
        {
            return _columns.FirstOrDefault(c => c.FieldName.Equals(fieldName, StringComparison.OrdinalIgnoreCase));
        }

        public int GetAvgSize(string fieldName)
        {
            var column = GetColumn(fieldName);
            if (column == null)
            {
                throw new ArgumentOutOfRangeException("fieldName");
            }
            if (column.FieldType == EFieldType.Symbol) return INT32_SIZE;

            var avgSize = column.AvgSize;
            if (avgSize < 0) avgSize = MetadataConstants.DEFAULT_AVG_RECORD_SIZE;
            return avgSize;
        }

        public void OverridePartitionType(EPartitionType value)
        {
            _partitionType = value;
        }

        public bool HasTimestamp()
        {
            return !string.IsNullOrEmpty(TimestampColumn);
        }
    }
}