using System;
using Apaf.NJournal.Core.Column;

namespace Apaf.NJournal.Core
{
    public class JournalSettings
    {
        private readonly string _defaultPath;
        private readonly string _timestampColumn;
        private readonly string _keySymbol;
        private readonly EPartitionType _partitionType;
        private readonly int _recordHit;
        private readonly int _openPartitionTTL;
        private readonly int _maxOpenPartitions;
        private readonly int _lagHours;
        private readonly int _avgSize;
        private readonly int _bitHint;

        public JournalSettings(string defaultPath,
            string timestampColumn,
            string keySymbol,
            EPartitionType partitionType,
            int openPartitionTtl,
            int maxOpenPartitions,
            int lagHours,
            int recordHit = -1,
            int avgSize = -1
            )
        {
            _defaultPath = defaultPath;
            _timestampColumn = timestampColumn;
            _keySymbol = keySymbol;
            _partitionType = partitionType;
            _openPartitionTTL = openPartitionTtl;
            _maxOpenPartitions = maxOpenPartitions;
            _lagHours = lagHours;

            _recordHit = recordHit;
            if (_recordHit < 0) _recordHit = MetadataConstants.DEFAULT_RECORD_HINT;
            
            _avgSize = avgSize;
            if (_avgSize < 0) _avgSize = MetadataConstants.AVG_RECORD_SIZE;

            // Size of read chunks. Rounded to power of 2.
            _bitHint = (int)Math.Ceiling(Math.Log(checked(_avgSize*_recordHit), 2));
        }

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

        public int RecordHit
        {
            get { return _recordHit; }
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

        public int AvgSize
        {
            get { return _avgSize; }
        }

        public int BitHint
        {
            get { return _bitHint; }
        }
    }
}