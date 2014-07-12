using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core
{
    public interface IJournalMetadata<T> : IJournalMetadataCore
    {
        void InitializeSymbols(IColumnStorage symbolStorage);
        IEnumerable<IColumn> GetPartitionColums(IColumnStorage partitionStorage);
        Func<T, long> GetTimestampReader();
    }
}