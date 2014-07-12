using System;
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.TestShared
{
    public class PartitionData<T>
    {
        private JournalMetadata<T> _metadata;
        private ColumnStorage _journalStorage;
        private string _journalPath;

        public PartitionData(Partition<T> partition,
            JournalMetadata<T> metadata, ColumnStorage journalStorage,
            string journalPath)
        {
            Partition = partition;
            Metadata = metadata;
            JournalStorage = journalStorage;
            JournalPath = journalPath;
        }

        public string JournalPath
        {
            get { return _journalPath; }
            set { _journalPath = value; }
        }

        public ColumnStorage JournalStorage
        {
            get { return _journalStorage; }
            set { _journalStorage = value; }
        }

        public Partition<T> Partition { get; set; }

        public JournalMetadata<T> Metadata
        {
            get { return _metadata; }
            set { _metadata = value; }
        }


        public ITransactionContext ReadTxLog()
        {
            var tx = new TransactionContext(_metadata.Columns.Count());
            ReadTxLogFromPartition(Partition.PartitionID, Partition.Storage, tx);
            ReadTxLogFromPartition(MetadataConstants.SYMBOL_PARTITION_ID, _journalStorage, tx);
            return tx;
        }

        public void ReadTxLogFromPartition(int partitionID, IColumnStorage files,
            TransactionContext tx)
        {
            throw new NotImplementedException();
        }
    }
}