using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core
{
    public interface IJournal<T> : IDisposable, IJournalCore
    {
        T Read(long rowID, IReadContext readContext);
        IEnumerable<T> Read(IEnumerable<long> rowIDs, IReadContext readContext);
        IComparer<long> GetRecordsComparer(int[] columnIndices);
        IQuery<T> OpenReadTx();
        IWriter<T> OpenWriteTx();

        IJournalMetadata<T> Metadata { get; }
        IEnumerable<IPartition<T>> Partitions { get; }
    }
}