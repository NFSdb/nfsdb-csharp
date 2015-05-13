using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IPartitionTxSupport
    {
        PartitionTxData GetPartitionTx(int partitionID, TxRec txRec);
        ILockedParititionReader ReadLock(int paritionID);
    }
}