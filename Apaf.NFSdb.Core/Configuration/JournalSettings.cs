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
        private readonly int _openPartitionTtl;
        private EPartitionType _partitionType;
        private readonly long _recordHint;
        private readonly string _timestampColumn;

        public JournalSettings(JournalElement jconf, IEnumerable<ColumnMetadata> actualColumns)
        {
            _defaultPath = jconf.DefaultPath;
            _timestampColumn = jconf.TimestampColumn;
            _keySymbol = jconf.Key;
            _partitionType = jconf.PartitionType;
            _openPartitionTtl = jconf.OpenPartitionTtl;
            _maxOpenPartitions = jconf.MaxOpenPartitions;
            _lagHours = jconf.LagHours;
            _columns = actualColumns.ToArray();
            Columns = _columns;
            _recordHint = jconf.RecordHint;
            if (_recordHint <= 0) _recordHint = MetadataConstants.DEFAULT_RECORD_HINT;
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

        public long RecordHint
        {
            get { return _recordHint; }
        }

        public int OpenPartitionTtl
        {
            get { return _openPartitionTtl; }
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
            return _columns.FirstOrDefault(c => c.FileName.Equals(fieldName, StringComparison.OrdinalIgnoreCase));
        }

        public int GetAvgSize(string fieldName)
        {
            var column = GetColumn(fieldName);
            if (column == null)
            {
                throw new ArgumentOutOfRangeException("fieldName");
            }

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