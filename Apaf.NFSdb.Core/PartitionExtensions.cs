using System;

namespace Apaf.NFSdb.Core
{
    public static class PartitionExtensions
    {
        public static bool IsInsidePartition(this IPartitionCore partition, DateTime timestamp)
        {
            return timestamp >= partition.StartDate && timestamp < partition.EndDate;
        }
    }
}