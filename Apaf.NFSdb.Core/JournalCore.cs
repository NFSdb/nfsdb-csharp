using System;
using System.IO;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Storage.Serializer.Records;

namespace Apaf.NFSdb.Core
{
    public class JournalCore : IJournalCore
    {
        private readonly IPartitionManager _partitionManager;
        private readonly JournalSettings _settings;

        internal JournalCore(IJournalMetadata metadata, IPartitionManager partitionManager)
        {
            _settings = metadata.Settings;
            _partitionManager = partitionManager;
            Metadata= metadata;
            var unsafePartitionManager = (IUnsafePartitionManager)partitionManager;
            QueryStatistics = new JournalStatistics(Metadata, unsafePartitionManager);
            Diagnostics = new JournalDiagnostics(unsafePartitionManager);
            Initialize(partitionManager.Access);
        }

        private void Initialize(EFileAccess access)
        {
            var di = new DirectoryInfo(_settings.DefaultPath);
            if (access == EFileAccess.ReadWrite && !di.Exists)
            {
                di.Create();
            }
            ConfigurePartitionType(access);
            if (access == EFileAccess.ReadWrite)
            {
                string settingsFile = Path.Combine(di.FullName, MetadataConstants.JOURNAL_SETTINGS_FILE_NAME);
                if (!File.Exists(settingsFile))
                {
                    using (var dbXml = File.Open(settingsFile, FileMode.Create, FileAccess.Write))
                    {
                        _settings.SaveTo(dbXml);
                    }
                }
            }
        }

        private void ConfigurePartitionType(EFileAccess access)
        {
            var partitionType = ReadPartitionType();

            if (!partitionType.HasValue)
            {
                partitionType = _settings.PartitionType;
                if (access == EFileAccess.ReadWrite)
                {
                    WritePartitionType(partitionType.Value);
                }
            }
            else
            {
                _settings.OverridePartitionType(partitionType.Value);
            }
        }

        private void WritePartitionType(EPartitionType value)
        {
            var path = Path.Combine(_settings.DefaultPath, MetadataConstants.PARTITION_TYPE_FILENAME);
            File.WriteAllText(path, value.ToString().ToUpper());
        }

        private EPartitionType? ReadPartitionType()
        {
            var path = Path.Combine(_settings.DefaultPath, MetadataConstants.PARTITION_TYPE_FILENAME);
            try
            {
                if (File.Exists(path))
                {
                    EPartitionType val;
                    if (Enum.TryParse(File.ReadAllText(path), true, out val))
                    {
                        return val;
                    }
                }
            }
            catch (IOException)
            {
            }
            return null;
        }

        public IJournalMetadata Metadata { get; private set; }
        public IQueryStatistics QueryStatistics { get; private set; }
        public IJournalDiagnostics Diagnostics { get; private set; }

        public IRecordQuery OpenRecordReadTx()
        {
            var txCntx = _partitionManager.ReadTxLog(Metadata.PartitionTtl.Milliseconds);
            return new RecordQuery(this, txCntx);
        }

        public void Dispose()
        {
            _partitionManager.Dispose();
        }
    }
}