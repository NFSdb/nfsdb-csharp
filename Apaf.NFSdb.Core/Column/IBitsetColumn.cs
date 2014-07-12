using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public interface IBitsetColumn : IColumn
    {
        byte[] GetValue(long rowID, IReadContext readContext);
        void SetValue(long rowID, byte[] bitArray, ITransactionContext readContext);
        int GetByteSize();
    }
}