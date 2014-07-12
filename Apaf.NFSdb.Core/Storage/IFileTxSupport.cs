using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IFileTxSupport
    {
        void ReadTxLogFromPartition(ITransactionContext tx, TxRec txRec);
        void Commit(ITransactionContext newTx, ITransactionContext previousTx);
        void SetTxRec(ITransactionContext tx, TxRec rec);
    }
}