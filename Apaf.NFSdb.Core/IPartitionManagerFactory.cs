using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core
{
    public interface IPartitionManagerFactory<T>
    {
        IPartitionManager<T> Create(IJournalMetadata<T> metadata,
            JournalSettings settings, EFileAccess access);
    }
}
