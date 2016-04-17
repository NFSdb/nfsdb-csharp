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
using System.Collections.Generic;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public class SymbolMapColumn : ISymbolMapColumn
    {
        private const int NOT_FOUND_VALUE = MetadataConstants.SYMBOL_NOT_FOUND_VALUE;
        private const int INT32_SIZE = 4;
        private const int STRING_INDEX_FILE_RECORD_SIZE = MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE;
        private static readonly int HASH_FUNCTION_GROUPING_RATE = MetadataConstants.HASH_FUNCTION_GROUPING_RATE;
        private const int STRING_HASH_CODE_SOLT = MetadataConstants.STRING_HASH_CODE_SOLT;
        private static readonly long[] EMPTY_RESULT = new long[0];

        private readonly int _columnId;
        private readonly int _partitionID;
        private readonly IRawFile _data;
        private readonly IRawFile _symi;
        private readonly StringColumn _globalSymColumn;
        private readonly int _capacity;
        private readonly int _cacheCapacity;

        private readonly int _symiFileID;
        private readonly IndexColumn _symrIndex;
        private readonly IndexColumn _datarIndex;
        private readonly bool _isIndexed;

        public SymbolMapColumn(int columnID, int partitionID, IRawFile data, 
            IRawFile symd, IRawFile symi,
            IRawFile symk, IRawFile symr,
            string propertyName, int capacity, int maxLen)
        {
            capacity = Math.Max(capacity, MetadataConstants.MIN_SYMBOL_DISTINCT_COUNT);
            _symiFileID = symi.FileID;
            _globalSymColumn = new StringColumn(symd, symi, maxLen, propertyName);
            _symrIndex = new IndexColumn(symk, symr, capacity, capacity * HASH_FUNCTION_GROUPING_RATE);

            _columnId = columnID;
            _partitionID = partitionID;
            _data = data;
            _symi = symi;
            _capacity = capacity;
            PropertyName = propertyName;
            FieldType = EFieldType.Symbol;

            _cacheCapacity = Math.Min(MetadataConstants.SYMBOL_STRING_CACHE_SIZE, capacity);
        }

        public SymbolMapColumn(int columnID, int partitionID, IRawFile data, IRawFile datak, IRawFile datar,
            IRawFile symd, IRawFile symi, 
            IRawFile symk, IRawFile symr, 
            string propertyName, int capacity, long recordCountHint, int maxLen) : this(columnID, partitionID, data, symd, symi, symk, symr, propertyName, capacity, maxLen)
        {
            _datarIndex = new IndexColumn(datak, datar, capacity, recordCountHint);
            _isIndexed = true;
        }

        public string GetString(long rowID, ReadContext readContext)
        {
            var symRowID = _data.ReadInt32(rowID * INT32_SIZE);
            if (symRowID == MetadataConstants.NULL_SYMBOL_VALUE)
            {
                return null;
            }

#if DEBUG
            if (symRowID < 0)
            {
                throw new IndexOutOfRangeException(_data.Filename + "|" + PropertyName + "|" + rowID + "\n" +
                                                   ((CompositeRawFile) _data).GetAllUnsafePointers()
                                                   + "\n" + ((CompositeRawFile)_data).GetAllBufferPointers());
            }
#endif

            var symbolCache = readContext.GetCache(_partitionID, _columnId, _capacity);
            string value;
            if (symbolCache.IsValueCached(symRowID, out value))
            {
                return value;
            }

            value = _globalSymColumn.GetString(symRowID, readContext);
            symbolCache.SetRowID(value, symRowID);
            return value;
        }

        public void SetString(long rowID, string value, PartitionTxData tx)
        {
            SetValue0(rowID, value, tx);
        }

        private void SetValue0(long rowID, string value, PartitionTxData tx)
        {
            var symbolCache = tx.ReadCache.GetCache(_partitionID, _columnId, _cacheCapacity);
            var key = symbolCache.GetRowID(value);
            if (key == NOT_FOUND_VALUE)
            {
                key = AddKey(value, symbolCache, tx);
            }

            _data.WriteInt32(rowID*INT32_SIZE, key);
            if (_isIndexed)
            {
                _datarIndex.Add(key, rowID, tx);
            }
        }

        private int AddKey(string value, SymbolCache symbolCache, PartitionTxData tx)
        {
            var hashKey = HashKey(value, _capacity);
            var key = CheckKeySlow(value, hashKey, symbolCache, tx);
            if (key == NOT_FOUND_VALUE)
            {
                var appendOffset = tx.AppendOffset[_symiFileID];
                key = (int) (appendOffset/STRING_INDEX_FILE_RECORD_SIZE);

                _globalSymColumn.SetString(key, value, tx);
                _symrIndex.Add(hashKey, key, tx);

                if (value == null)
                {
                    key = MetadataConstants.NULL_SYMBOL_VALUE;
                }
                symbolCache.SetRowID(value, key);
            }
            return key;
        }

        public object GetValue(long rowID, ReadContext readContext)
        {
            return GetString(rowID, readContext);
        }

        public void SetValue(long rowID, object value, PartitionTxData readContext)
        {
            SetValue0(rowID, (string)value, readContext);
        }

        public int CheckKeyQuick(string value, PartitionTxData tx)
        {
            var symbolCache = tx.ReadCache.GetCache(_partitionID, _columnId, _cacheCapacity);
            var key = symbolCache.GetRowID(value);
            if (key == MetadataConstants.SYMBOL_NOT_FOUND_VALUE)
            {
                var hashKey = HashKey(value, _capacity);
                return CheckKeySlow(value, hashKey, symbolCache, tx);
            }
            return key;
        }

        public int GetDistinctCount(PartitionTxData tx)
        {
            return (int) (tx.AppendOffset[_symi.FileID]/MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE);
        }

        public string GetKeyValue(int key, PartitionTxData rc)
        {
            return _globalSymColumn.GetString(key, rc.ReadCache);
        }

        public string GetKeyValue(int keyId, ReadContext tx)
        {
            var symbolCache = tx.GetCache(_partitionID, _columnId, _cacheCapacity);
            string value;
            if (symbolCache.IsValueCached(keyId, out value))
            {
                return value;
            }
            value = _globalSymColumn.GetString(keyId, tx);
            symbolCache.SetRowID(value, keyId);
            return value;
        }

        public IEnumerable<long> GetValues(int valueKey, PartitionTxData tx)
        {
            if (_datarIndex == null)
            {
                throw new NFSdbConfigurationException("Index does not exists for column "
                                                      + _globalSymColumn.PropertyName);
            }
            if (valueKey == NOT_FOUND_VALUE)
            {
                return EMPTY_RESULT;
            }
            return _datarIndex.GetValues(valueKey, tx);
        }

        public long GetCount(int valueKey, PartitionTxData tx)
        {
            return _datarIndex.GetCount(valueKey, tx);
        }

        private int CheckKeySlow(string value, int hashKey, SymbolCache symbolCache, PartitionTxData tx)
        {
            var values = _symrIndex.GetValues(hashKey, tx);
            foreach (var possibleKey in values)
            {
                var key = (int) possibleKey;
                string possibleValue;
                if (!symbolCache.IsValueCached(hashKey, out possibleValue))
                {
                    possibleValue = _globalSymColumn.GetString(key, tx.ReadCache);
                    symbolCache.SetRowID(possibleValue, key);
                    if (string.Equals(value, possibleValue, StringComparison.Ordinal))
                    {
                        if (value == null)
                        {
                            key = MetadataConstants.NULL_SYMBOL_VALUE;
                        }
                        return key;
                    }
                }
            }
            return NOT_FOUND_VALUE;
        }

        public static unsafe int HashKey(string value, int capacity)
        {
            if (value == null) return MetadataConstants.NULL_SYMBOL_VALUE;
            fixed (char* strPtr = value)
            {
                int hash = 0;
                unchecked
                {
                    for (int i = 0; i < value.Length; i++)
                    {
                        hash = 31*hash + strPtr[i];
                    }
                }
                return (hash & STRING_HASH_CODE_SOLT) % capacity;
            }
        }

        public EFieldType FieldType { get; private set; }
        public string PropertyName { get; private set; }

        public string Get(long rowID, ReadContext readContext)
        {
            return GetString(rowID, readContext);
        }
    }
}