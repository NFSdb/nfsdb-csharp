using System;

namespace Apaf.NFSdb.Core.Storage
{
    public interface ICompositeFile : IDisposable
    {
        IRawFilePart CreateViewAccessor(long offset, long size);
        string Filename { get; }
        long Size { get; }
    }
}