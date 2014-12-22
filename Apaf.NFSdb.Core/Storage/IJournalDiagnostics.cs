namespace Apaf.NFSdb.Core.Storage
{
    public interface IJournalDiagnostics
    {
        int GetTotalFilesOpen();
        long GetTotalMemoryMapped();
    }
}