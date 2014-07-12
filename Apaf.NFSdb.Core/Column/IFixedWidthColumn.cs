using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public interface IFixedWidthColumn : IColumn
    {
        int GetInt32(long rowID);
        long GetInt64(long rowID);
        short GetInt16(long rowID);
        byte GetByte(long rowID);
        bool GetBool(long rowID);
        double GetDouble(long rowID);

        void SetInt32(long rowID, int value, ITransactionContext readContext);
        void SetInt64(long rowID, long value, ITransactionContext readContext);
        void SetInt16(long rowID, short value, ITransactionContext readContext);
        void SetByte(long rowID, byte value, ITransactionContext readContext);
        void SetBool(long rowID, bool value, ITransactionContext readContext);
        void SetDouble(long rowID, double value, ITransactionContext readContext);
    }
}