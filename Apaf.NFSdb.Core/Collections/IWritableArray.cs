namespace Apaf.NFSdb.Core.Collections
{
    public interface IWritableArray<T> : IArray<T>
    {
        void GetArray(out T[] array, out int index, out int count);
    }
}