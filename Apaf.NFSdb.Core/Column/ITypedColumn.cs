using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Column
{
    public interface ITypedColumn<T>
    {
        T Get(long rowID, IReadContext readContext);
    }
}