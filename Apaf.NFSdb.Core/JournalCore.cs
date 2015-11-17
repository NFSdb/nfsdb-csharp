using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Queries.Records;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core
{
    public class JournalCore : IJournalCore
    {
        private readonly IPartitionManagerCore _partitionManager;

        internal JournalCore(IJournalMetadataCore metadata,
            IPartitionManagerCore partitionManager)
        {
            _partitionManager = partitionManager;
            MetadataCore = metadata;
            var unsafePartitionManager = (IUnsafePartitionManager)partitionManager;
            QueryStatistics = new JournalStatistics(MetadataCore, unsafePartitionManager);
            Diagnostics = new JournalDiagnostics(unsafePartitionManager);
        }

        public IJournalMetadataCore MetadataCore { get; private set; }
        public IQueryStatistics QueryStatistics { get; private set; }
        public IJournalDiagnostics Diagnostics { get; private set; }

        public IRecordQuery OpenRecordReadTx()
        {
            var txCntx = _partitionManager.ReadTxLog(MetadataCore.PartitionTtl.Milliseconds);
            return new RecordQuery(this, txCntx);
        }
    }
}