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
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Storage
{
    public sealed class ReadContext
    {
        private byte[] _arr1;
        private byte[] _arr3;
        private ObjIntHashMap _columnNames = new ObjIntHashMap();
        private SymbolCache[][] _symbolCaches = new SymbolCache[MetadataConstants.PRE_ALLOCATED_PARTITIONS][];

        public byte[] AllocateBitsetArray(int size)
        {
            return _arr1 ?? (_arr1 = new byte[size]);
        }

        public byte[] AllocateCopyKeyBlockArray(int size)
        {
            return _arr3 != null && _arr3.Length >= size ? _arr3 : (_arr3 = new byte[size]);
        }

        public SymbolCache GetCache(int partitionId, int columnId, int capacity)
        {
            if (partitionId < _symbolCaches.Length)
            {
                var caches = _symbolCaches[partitionId];
                if (caches != null && columnId < caches.Length)
                {
                    SymbolCache cache;
                    if ((cache = caches[columnId]) != null) return cache;
                }
            }
            return GetCacheSlow(partitionId, columnId, capacity);
        }

        private SymbolCache GetCacheSlow(int partitionId, int columnId, int capacity)
        {
            if (_symbolCaches.Length <= partitionId)
            {
                 var extended = new SymbolCache[partitionId + MetadataConstants.PRE_ALLOCATED_PARTITIONS][];
                Array.Copy(_symbolCaches, extended, _symbolCaches.Length);
                _symbolCaches = extended;
            }

            SymbolCache[] columns = _symbolCaches[partitionId];
            if (columns == null)
            {
                columns = new SymbolCache[Math.Max(columnId, MetadataConstants.PRE_ALLOCATED_COLUMNS)];
                _symbolCaches[partitionId] = columns;
            }

            if (columns.Length <= columnId)
            {
                var newColumns = new SymbolCache[
                    (columnId / MetadataConstants.PRE_ALLOCATED_COLUMNS + 1) * MetadataConstants.PRE_ALLOCATED_COLUMNS];
                Array.Copy(columns, newColumns, columns.Length);
                columns = newColumns;
                _symbolCaches[partitionId] = newColumns;
            }

            var cache = columns[columnId];
            if (cache == null)
            {
                cache = new SymbolCache(capacity);
                columns[columnId] = cache;
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