﻿#region copyright
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
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public class SymbolMapColumn : ISymbolMapColumn
    {
        private const int NOT_FOUND_VALUE = MetadataConstants.SYMBOL_NOT_FOUND_VALUE;
        private const int INT32_SIZE = 4;
        private const int SYMI_PARTITION_ID = MetadataConstants.SYMBOL_PARTITION_ID;
        private const int STRING_INDEX_FILE_RECORD_SIZE = MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE;
        private static readonly int HASH_FUNCTION_GROUPING_RATE = MetadataConstants.HASH_FUNCTION_GROUPING_RATE;
        private const int STRING_HASH_CODE_SOLT = MetadataConstants.STRING_HASH_CODE_SOLT;
        private static readonly long[] EMPTY_RESULT = new long[0];

        private readonly IRawFile _data;
        private readonly StringColumn _globalSymColumn;
        private readonly int _capacity;
        private readonly int _symiFileID;
        private readonly IndexColumn _symrIndex;
        private readonly SymbolCache _symbolCache;
        private readonly IndexColumn _datarIndex;
        private readonly bool _isIndexed;

        public SymbolMapColumn(IRawFile data, 
            IRawFile symd, IRawFile symi,
            IRawFile symk, IRawFile symr,
            string propertyName, int capacity, int maxLen,
            SymbolCache symbolCache)
        {
            capacity = Math.Max(capacity, MetadataConstants.MIN_SYMBOL_DISTINCT_COUNT);
            _symiFileID = symi.FileID;
            _globalSymColumn = new StringColumn(symd, symi, maxLen, propertyName);
            _symrIndex = new IndexColumn(symk, symr, capacity, capacity * HASH_FUNCTION_GROUPING_RATE);

            _data = data;
            _capacity = capacity;
            _symbolCache = symbolCache;
            PropertyName = propertyName;
            FieldType = EFieldType.Symbol;
            symbolCache.SetValueCacheCapacity(Math.Min(MetadataConstants.SYMBOL_STRING_CACHE_SIZE, capacity));
        }

        public SymbolMapColumn(IRawFile data, IRawFile datak, IRawFile datar,
            IRawFile symd, IRawFile symi, 
            IRawFile symk, IRawFile symr, 
            string propertyName, int capacity, int recordCountHint, int maxLen, 
            SymbolCache symbolCache) : this(data, symd, symi, symk, symr, propertyName, capacity, maxLen, symbolCache)
        {
            _datarIndex = new IndexColumn(datak, datar, capacity, recordCountHint);
            _isIndexed = true;
        }

        public string GetString(long rowID, IReadContext readContext)
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

            if (_symbolCache.IsValueCached(symRowID))
            {
                return _symbolCache.GetCachedValue(symRowID);
            }

            var value = _globalSymColumn.GetString(symRowID, readContext);
            _symbolCache.AddSymbolValue(symRowID, value);
            return value;
        }

        public void SetString(long rowID, string value, ITransactionContext tx)
        {
            int key;
            if (value != null)
            {
                key = _symbolCache.GetRowID(value);
                if (key == NOT_FOUND_VALUE)
                {
                    key = AddKey(value, tx);
                }
            }
            else
            {
                key = MetadataConstants.NULL_SYMBOL_VALUE;
            }
            _data.WriteInt32(rowID * INT32_SIZE, key);
            if (_isIndexed)
            {
                _datarIndex.Add(key, rowID, tx);
            }
        }

        private int AddKey(string value, ITransactionContext tx)
        {
            var hashKey = HashKey(value, _capacity);
            var key = CheckKeyQuick(value, hashKey, tx);
            if (key == NOT_FOUND_VALUE)
            {
                var partTx = tx.GetPartitionTx(SYMI_PARTITION_ID);
                var appendOffset = partTx.AppendOffset[_symiFileID];
                key = (int) (appendOffset/STRING_INDEX_FILE_RECORD_SIZE);

                _globalSymColumn.SetString(key, value, partTx);
                _symrIndex.Add(hashKey, key, tx);
            }
            _symbolCache.SetRowID(value, key);
            return key;
        }

        public object GetValue(long rowID, IReadContext readContext)
        {
            return GetString(rowID, readContext);
        }

        public void SetValue(long rowID, object value, ITransactionContext readContext)
        {
            SetString(rowID, (string)value, readContext);
        }

        public int CheckKeyQuick(string value, IReadTransactionContext tx)
        {
            var key = _symbolCache.GetRowID(value);
            if (key < 0)
            {
                var hashKey = HashKey(value, _capacity);
                return CheckKeyQuick(value, hashKey, tx);
            }
            return key;
        }

        public IEnumerable<long> GetValues(int valueKey, IReadTransactionContext tx)
        {
            if (valueKey == NOT_FOUND_VALUE)
            {
                return EMPTY_RESULT;
            }
            return _datarIndex.GetValues(valueKey, tx);
        }

        public long GetCount(int valueKey, IReadTransactionContext tx)
        {
            return _datarIndex.GetCount(valueKey, tx);
        }

        private int CheckKeyQuick(string value, int hasKey, IReadTransactionContext tx)
        {
            var values = _symrIndex.GetValues(hasKey, tx);
            foreach (var possibleKey in values)
            {
                var key = (int) possibleKey;
                var possibleValue = _globalSymColumn.GetString(key, tx.ReadCache);
                if (string.Equals(value, possibleValue, StringComparison.Ordinal))
                {
                    _symbolCache.SetRowID(value, key);
                    return key;
                }
            }
            return NOT_FOUND_VALUE;
        }

        public static unsafe int HashKey(string value, int capacity)
        {
            if (string.IsNullOrEmpty(value)) return 0;
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
    }
}