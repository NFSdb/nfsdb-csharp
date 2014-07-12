namespace Apaf.NFSdb.Core.Storage
{
    public class CompositeFileFactory : ICompositeFileFactory
    {
        public ICompositeFile OpenFile(string filename, int bitHint, EFileAccess access)
        {
            return new MemoryFile(filename, bitHint, access);
        }
    }
}