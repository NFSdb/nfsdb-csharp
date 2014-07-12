using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core
{
    public interface IFieldSerializer 
    {
        object Read(long rowID, IReadContext readContext);
        void Write(object item, long rowID, ITransactionContext readContext);
    }
}