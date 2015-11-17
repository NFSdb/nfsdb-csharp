namespace Apaf.NFSdb.Core.Collections
{
    public interface IObjIntHashMap
    {
        int Get(string key);
        int Put(string key, int value);
        bool PutIfAbsent(string key, int value);
        void Clear();
        int Size();
    }
}