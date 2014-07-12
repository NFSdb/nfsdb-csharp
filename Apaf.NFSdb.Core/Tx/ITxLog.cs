namespace Apaf.NFSdb.Core.Tx
{
    public interface ITxLog
    {
        bool IsEmpty();
        TxRec Get();
        void Create(TxRec tx);
    }
}