using Apaf.NFSdb.Core.Collections;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IBufferManager
    {
        IWritableArray<byte> GetBuffer(int size);
    }
}