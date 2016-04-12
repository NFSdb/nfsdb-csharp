using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Storage.Serializer.Records;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IPartitionCompressorCore
    {
        void UpdatePartition(Func<IRecordSet, IEnumerable<long>> recordsToKeep);
    }
}