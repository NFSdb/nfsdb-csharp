using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public interface IStringColumn : IRefTypeColumn, ITypedColumn<string>
    {
        string GetString(long rowID, ReadContext readContext);
        void SetString(long rowID, string value, PartitionTxData readContext); 
    }
}