namespace Apaf.NFSdb.Core.Storage
{
    public interface ICompositeFileFactory
    {
        ICompositeFile OpenFile(string filename, int bitHint, EFileAccess fileAccess);
    }
}