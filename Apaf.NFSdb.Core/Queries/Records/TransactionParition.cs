using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Queries.Records
{
    public class TransactionParition
    {
        private readonly IJournalMetadataCore _metadata;

        public readonly IPartitionCore Partition;
        public readonly IReadContext ReadContext;

        public TransactionParition(IPartitionCore partition, 
            IReadContext readContext, IJournalMetadataCore metadata)
        {
            Partition = partition;
            ReadContext = readContext;
            _metadata = metadata;
        }

        public int GetColumnID(string name)
        {
            var index = ReadContext.ColumnNames.Get(name);
            if (index != MetadataConstants.SYMBOL_NOT_FOUND_VALUE) return index;

            index = _metadata.GetColumnID(name);
            ReadContext.ColumnNames.Put(name, index);

            return index;
        }
    }
}