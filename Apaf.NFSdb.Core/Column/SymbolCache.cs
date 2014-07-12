using System.Collections.Generic;
using Apaf.NFSdb.Core.Exceptions;

namespace Apaf.NFSdb.Core.Column
{
    public class SymbolCache
    {
        private readonly Dictionary<string, int> _cache = new Dictionary<string, int>();
        private string[] _symbolCache;
        private int[] _cachedKeys;
        private int _cacheCapacity;

        public int GetRowID(string key)
        {
            int value;
            return !_cache.TryGetValue(key, out value) ? -1 : value;
        }

        public void SetRowID(string key, int value)
        {
            _cache[key] = value;
        }

        public void Reset()
        {
            _cache.Clear();
        }

        public bool IsValueCached(int cacheIndex)
        {
            // 0 is default. Increment expected value by 1 to avoid array initialization.
            var index = cacheIndex%_cacheCapacity;
            return _cachedKeys[index] == cacheIndex + 1;
        }

        public string GetCachedValue(int cacheIndex)
        {
            var index = cacheIndex % _cacheCapacity;
            return _symbolCache[index];
        }

        public void SetValueCacheCapacity(int capacity)
        {
            _cacheCapacity = capacity;
            if (_cachedKeys != null && _cachedKeys.Length != capacity)
            {
                throw new NFSdbPartitionException("Symbol cache is initialized using different capcity");
            }
            if (_cachedKeys == null)
            {
                _cachedKeys = new int[capacity];
                _symbolCache = new string[capacity];
            }
        }

        public void AddSymbolValue(int symRowID, string value)
        {
            var index = symRowID%_cacheCapacity;
            _cachedKeys[index] = symRowID + 1;
            _symbolCache[index] = value;
        }
    }
}