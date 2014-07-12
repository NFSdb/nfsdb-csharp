using System.Collections.Generic;

namespace Apaf.NFSdb.Core.Collections
{
    public interface IArray<T> : IEnumerable<T>
    {
        int Count { get; }
        T this[int index] { get; }
    }
}