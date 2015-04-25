using System;
using System.Linq;

namespace Apaf.NFSdb.Core.Storage
{
    public class JournalDiagnostics : IJournalDiagnostics
    {
        private readonly IPartitionManagerCore _partitionManager;

        public JournalDiagnostics(IPartitionManagerCore partitionManager)
        {
            if (partitionManager == null) throw new ArgumentNullException("partitionManager");
            _partitionManager = partitionManager;
        }

        public int GetTotalFilesOpen()
        {
            int totalCount = 0;
            foreach (var partition in _partitionManager.Partitions)
            {
                if (partition != null)
                {
                    var storage = partition.Storage;
                    if (storage != null)
                    {
                        var allFiles = storage.AllOpenedFiles();
                        if (allFiles != null)
                        {
                            totalCount += allFiles.Count(f => f.MappedSize > 0);
                        }
                    }
                }
            }
            return totalCount;
        }

        public long GetTotalMemoryMapped()
        {
            long totalMemory = 0;
            foreach (var partition in _partitionManager.Partitions)
            {
                if (partition != null)
                {
                    var storage = partition.Storage;
                    if (storage != null)
                    {
                        var allFiles = storage.AllOpenedFiles();
                        if (allFiles != null)
                        {
                            totalMemory += allFiles.Sum(s => s.MappedSize);
                        }
                    }
                }
            }
            return totalMemory;
        }
    }
}