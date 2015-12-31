using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Server
{
    public interface IJournalServer
    {
        void SignalUnusedPartition(IPartition partition, int offloadTimeoutTtl);
    }
}