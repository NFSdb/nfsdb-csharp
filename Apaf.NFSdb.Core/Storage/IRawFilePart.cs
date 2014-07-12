namespace Apaf.NFSdb.Core.Storage
{
    public interface IRawFilePart : IRawFile
    {
        long BufferSize { get; }
        long BufferOffset { get; }
    }
}