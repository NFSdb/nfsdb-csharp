using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;

namespace Apaf.NFSdb.Core.Storage.Serializer.Records
{
    public class TransactionParition
    {
        private readonly IJournalMetadata _metadata;

        public readonly IPartitionCore Partition;
        public readonly IReadContext ReadContext;

        public TransactionParition(IPartitionCore partition, 
            IReadContext readContext, IJournalMetadata metadata)
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