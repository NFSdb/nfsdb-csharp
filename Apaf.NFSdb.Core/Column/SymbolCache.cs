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

using Apaf.NFSdb.Core.Collections;

namespace Apaf.NFSdb.Core.Column
{
    public class SymbolCache
    {
        private readonly ObjIntHashMap _cache2;

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

        public bool IsValueCached(int cacheIndex, out string value)
        {
            return _cache2.LookupValue(cacheIndex, out value);
        }
    }
}