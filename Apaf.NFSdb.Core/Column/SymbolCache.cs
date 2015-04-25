#region copyright
/*
 * Copyright (c) 2014. APAF http://apafltd.co.uk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion

using System;
using Apaf.NFSdb.Core.Collections;
using Apaf.NFSdb.Core.Exceptions;

namespace Apaf.NFSdb.Core.Column
{
    public class SymbolCache
    {
        private readonly ObjIntHashMap _cache2;
        private string[] _symbolCache;
        private int[] _cachedKeys;
        private int _cacheCapacityBitHint;

        public SymbolCache()
        {
            _cache2 = new ObjIntHashMap();
        }

        public SymbolCache(int distinctCount)
        {
           _cache2 = new ObjIntHashMap(distinctCount * 2);
        }

        public int GetRowID(string key)
        {
            return _cache2.Get(key);
        }

        public void SetRowID(string key, int value)
        {
            _cache2.Put(key, value);
        }

        public void Reset()
        {
            _cache2.Clear();
        }

        public bool IsValueCached(int cacheIndex)
        {
            // 0 is default. Increment expected value by 1 to avoid array initialization.
            var index = cacheIndex & (_cacheCapacityBitHint - 1);
            return _cachedKeys[index] == cacheIndex + 1;
        }

        public string GetCachedValue(int cacheIndex)
        {
            var index = cacheIndex & (_cacheCapacityBitHint - 1);
            return _symbolCache[index];
        }

        public void SetValueCacheCapacity(int capacity)
        {
            _cacheCapacityBitHint = (int)Math.Floor(Math.Log(Math.Max(capacity, 16), 2.0));
            _cacheCapacityBitHint = Math.Max(_cacheCapacityBitHint, 4);
            capacity = 1 << _cacheCapacityBitHint;
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
            var index = symRowID & (_cacheCapacityBitHint - 1);
            _cachedKeys[index] = symRowID + 1;
            _symbolCache[index] = value;
        }
    }
}