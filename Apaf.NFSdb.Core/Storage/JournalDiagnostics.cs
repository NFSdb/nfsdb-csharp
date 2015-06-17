using System;

namespace Apaf.NFSdb.Core.Storage
{
    public class JournalDiagnostics : IJournalDiagnostics
    {
        private readonly IUnsafePartitionManager _partitionManager;

        internal JournalDiagnostics(IUnsafePartitionManager partitionManager)
        {
            if (partitionManager == null) throw new ArgumentNullException("partitionManager");
            _partitionManager = partitionManager;
        }

        public int GetTotalFilesOpen()
        {
            int totalCount = 0;
            foreach (var partition in _partitionManager.GetOpenPartitions())
            {
                if (partition != null)
                {
                    totalCount += partition.GetOpenFileCount();
                }
            }
            return totalCount;
        }

        public long GetTotalMemoryMapped()
        {
            long totalMemory = 0;
            foreach (var partition in _partitionManager.GetOpenPartitions())
            {
                if (partition != null)
                {
                    totalMemory += partition.GetTotalMemoryMapped();
                }
            }
            return totalMemory;
        }
    }
}