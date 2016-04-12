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

using System.Collections;
using Apaf.NFSdb.Core.Collections;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Storage
{
    public class ReadContext : IReadContext
    {
        private byte[] _arr1;
        private byte[] _arr3;
        private ObjIntHashMap _columnNames = new ObjIntHashMap();
        private readonly ArrayList _symbolCaches = new ArrayList();

        public byte[] AllocateByteArray(int size)
        {
            return _arr1 ?? (_arr1 = new byte[size]);
        }

        public byte[] AllocateByteArray2(int size)
        {
            return new byte[size];
        }

        public byte[] AllocateByteArray3(int size)
        {
            return _arr3 != null && _arr3.Length >= size ? _arr3 : (_arr3 = new byte[size]);
        }

        public SymbolCache GetCache(int partitionId, int columnId, int capacity)
        {
            if (partitionId < _symbolCaches.Count)
            {
                var caches = (ArrayList)_symbolCaches[partitionId];
                if (caches != null && columnId < caches.Count)
                {
                    SymbolCache cache;
                    if ((cache = (SymbolCache)caches[columnId]) != null) return cache;
                }
            }
            return GetCache0(partitionId, columnId, capacity);
        }

        private SymbolCache GetCache0(int partitionId, int columnId, int capacity)
        {
            ArrayList columns;
            if (_symbolCaches.Count <= partitionId || _symbolCaches[partitionId] == null)
            {
                columns = new ArrayList();
                _symbolCaches.SetToIndex(partitionId, columns);
            }
            else
            {
                columns = (ArrayList)_symbolCaches[partitionId];
            }

            SymbolCache cache;
            if (columns.Count <= columnId || (cache = (SymbolCache)columns[columnId]) == null)
            {
                cache = new SymbolCache();
                cache.SetValueCacheCapacity(capacity);
                columns.SetToIndex(columnId, cache);
                return cache;
            }
            return cache;
        }
        
        public ObjIntHashMap AllocateStringHash()
        {
            if (_columnNames != null)
            {
                _columnNames.Clear();
                return _columnNames;
            }
            return _columnNames = new ObjIntHashMap();
        }
    }
}