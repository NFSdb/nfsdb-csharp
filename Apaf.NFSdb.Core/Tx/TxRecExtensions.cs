using System;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core.Tx
{
    public static class TxRecExtensions
    {
        public static bool IsCommited(this TxRec txRec, DateTime partitionStartDate, int partitionID)
        {
            if (txRec == null)
            {
                return false;
            }

            if (txRec.LastPartitionTimestamp != 0)
            {
                return partitionStartDate <= DateUtils.UnixTimestampToDateTime(txRec.LastPartitionTimestamp);
            }

            var lastPartitionID = RowIDUtil.ToPartitionIDFromExternalRowID(txRec.JournalMaxRowID);
            return partitionID <= lastPartitionID;
        }
    }
}