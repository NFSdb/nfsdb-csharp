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
using System.Collections.Generic;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    // File binary structure
    // .r file
    // -------------------------------------------
    // Header:
    // -------------------------------------------
    // Append Offset (8)
    // .. values block .. values block .. values block ..
    //
    // -------------------------------------------
    // Values Block structure:
    // -------------------------------------------
    // .. Value (8) ... Value (8) ... Value(8) ... ... ... Prev Block End Offset (8)
    // -------------------------------------------
    public class IndexColumn
    {
        public static readonly int ENTRY_SIZE = 16;

        private readonly IRawFile _rData;
        private readonly IndexAddressColumn _indexAddress;
        private readonly int _rFileID;
        private readonly IRawFile _kData;
        private readonly int _rowBlockLen;
        private readonly int _rowBlockSize;
        private readonly int _rowBlockLenBitHint;

        public IndexColumn(IRawFile kData, IRawFile rData, int capacity, long recordCountHint)
        {
            _rFileID = rData.FileID;
            _kData = kData;
            _indexAddress = new IndexAddressColumn(kData, capacity, recordCountHint);
            _rowBlockLenBitHint = _indexAddress.RowBlockLenBitHint;
            _rowBlockLen = 1 << _rowBlockLenBitHint;
            _rowBlockSize = _indexAddress.RowBlockSize;
            _rData = rData;
        }

        public IEnumerable<long> GetValues(int hashKey, PartitionTxData tx)
        {
            var keyRecOffset = _indexAddress.ReadOnlyKeyRecordOffset(hashKey, tx);
            if (keyRecOffset < 0)
            {
                yield break;
            }
            var blockOffset = _kData.ReadInt64(keyRecOffset);
            var rowCount = _kData.ReadInt64(keyRecOffset + 8);

            // Key found.
            var rowBlockSize = _indexAddress.RowBlockSize;
            var rowBlockCount = (int)(rowCount >> _rowBlockLenBitHint) + 1;
            var len = (int)(rowCount & (_rowBlockLen - 1));
            if (len == 0)
            {
                rowBlockCount--;
                len = _rowBlockLen;
            }

            var rowBlockOffset = blockOffset;
            var partMaxRowId = tx.NextRowID;

            // Loop blocks.
            for (int i = rowBlockCount - 1; i >=0 ; i--)
            {
                // Loop cells.
                for (int k = len - 1; k >=0; k--)
                {
                    var rowid =  _rData.ReadInt64(rowBlockOffset - rowBlockSize + k * 8);
                    // yield return rowid;

                    // Check data visible.
                    if (partMaxRowId < 0 || rowid < partMaxRowId)
                    {
                        yield return rowid;
                    }
                }

                // Next block.
                rowBlockOffset = _rData.ReadInt64(rowBlockOffset - 8);
                len = _rowBlockLen;
            }
        }

        public long GetCount(int valueKey, PartitionTxData tx)
        {
            var keyRecOffset = _indexAddress.ReadOnlyKeyRecordOffset(valueKey, tx);
            if (keyRecOffset < 0)
            {
                return 0L;
            }

            // Key found.
            var rowCount = _kData.ReadInt64(keyRecOffset + 8);
            return rowCount;
        }

        public void Add(int key, long value, PartitionTxData tx)
        {
            var keyRecOffset = _indexAddress.ReadKeyRecordOffset(key, tx);
            var blockOffset = _kData.ReadInt64(keyRecOffset);
            long offset = keyRecOffset + 8;
            var rowCount = _kData.ReadInt64(offset);
            var cellIndex = (int)(rowCount & (_rowBlockLen - 1));
            
            if (blockOffset == 0 || cellIndex == 0)
            {
                var prevRowBlockOffset = blockOffset;
                blockOffset = tx.AppendOffset[_rFileID] + _rowBlockSize;
                tx.AppendOffset[_rFileID] = blockOffset;
                _rData.WriteInt64(blockOffset - 8, prevRowBlockOffset);
                
                // Save block offset in k file
                _kData.WriteInt64(keyRecOffset, blockOffset);
            }

            // Save value in index
            _rData.WriteInt64(blockOffset - _rowBlockSize + 8 * cellIndex, value);
            // Save row count in k file
            _kData.WriteInt64(offset, rowCount + 1);
        }
    }
}