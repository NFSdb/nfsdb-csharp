using System;

namespace Apaf.NFSdb.Core.Writes
{
    public interface IWriter<T> : IDisposable
    {
        void Append(T item);
        void Commit();
    }
}