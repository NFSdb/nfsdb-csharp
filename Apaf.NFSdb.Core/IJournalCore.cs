using System.Collections.Generic;

namespace Apaf.NFSdb.Core
{
    public interface IJournalCore
    {
        IEnumerable<IPartitionCore> PartitionsCore { get; }
        IQueryStatistics QueryStatistics { get; }
    }
}