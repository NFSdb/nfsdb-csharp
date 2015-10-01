using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public interface IColumnFilter : IPartitionFilter
    {
        ColumnMetadata Column { get; }
        bool IsMatch(IPartitionReader partition, IReadContext tx, long localRowID);
        long GetCardinality(IJournalCore journal, IReadTransactionContext tx);
    }
}