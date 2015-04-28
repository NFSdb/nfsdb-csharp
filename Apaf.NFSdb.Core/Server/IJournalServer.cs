using System;

namespace Apaf.NFSdb.Core.Server
{
    public interface IJournalServer
    {
        void SchedulePartitionAppendComplete(Action cleanAction);
    }
}