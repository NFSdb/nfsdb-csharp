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
using System.Diagnostics;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    // File binary structure
    // .k files
    // -------------------------------------------
    // Header:
    // -------------------------------------------
    // Row Block Len         (8)
    // Key Block Size Offset (8)
    // .. key block .. key block .. key block ..
    //
    // -------------------------------------------
    // Key Block structure:
    // -------------------------------------------
    // Key Block Size (8)
    // Max Value      (8)
    // .. key record .. key record .. key record..
    //
    // -------------------------------------------
    // Key record structure:
    // -------------------------------------------
    // Row Block Offset (8)
    // Row Count        (8)
    public class IndexAddressColumn
    {
        public struct KeyRecord
        {
            public long BlockOffset;
            public long RowCount;
            public long KeyOffset;

            public static KeyRecord Empty = new KeyRecord
            {
                BlockOffset = -1
            };
        }

        private const int EMPTY_BUFFER_SIZE = 1024;
        private const int KEY_BLOCK_ADDRESS_OFFSET = MetadataConstants.K_FILE_KEY_BLOCK_OFFSET;
        private const int ROW_BLOK_LEN_ADDRESS_OFFSET = MetadataConstants.K_FILE_ROW_BLOCK_LEN_OFFSET;
        private const int KEY_BLOCK_HEADER_SIZE = MetadataConstants.K_FILE_KEY_BLOCK_HEADER_SIZE;
        public static readonly int KEY_RECORD_ENTRY_SIZE = 16;
        private const int INITIAL_KEYBLOCK_OFFSET = MetadataConstants.K_FILE_KEY_BLOCK_OFFSET + 8;
        private const int INITIAL_APPEND_OFFSET = INITIAL_KEYBLOCK_OFFSET + MetadataConstants.K_FILE_KEY_BLOCK_HEADER_SIZE;
        private static readonly byte[] EMPTY_BUFFER = new byte[EMPTY_BUFFER_SIZE];

        private readonly IRawFile _kData;
        private readonly int _fileID;
        private readonly int _partitionID;

        private readonly int _rowBlockSize;
        private readonly int _rowBlockLenBitHint;

        public IndexAddressColumn(IRawFile kData, int capacity, long recordCountHint)
        {
            _fileID = kData.FileID;
            _partitionID = kData.PartitionID;
            int keycountHint = Math.Max(capacity, 1);

            // Nuber of records per keyblock
            // is a half of statistically expected
            // but minimum size is 10
            _rowBlockLenBitHint = (int)Math.Floor(Math.Log(Math.Max(recordCountHint / keycountHint / 2, 16), 2.0)) - 1;
            _rowBlockLenBitHint = Math.Max(_rowBlockLenBitHint, 4);
            int rowBlockLen = 1 << _rowBlockLenBitHint;
            _kData = kData;

            if (kData.GetAppendOffset() > 0)
            {
                rowBlockLen = _kData.ReadInt32(ROW_BLOK_LEN_ADDRESS_OFFSET);
                _rowBlockLenBitHint = (int)Math.Log(rowBlockLen, 2);

                if (1 << _rowBlockLenBitHint != rowBlockLen)
                {
                    throw new NFSdbInvalidStateException("Row block length specified in file '{0}' equals " +
                                                         "{1}. Only power of 2 values are supported.", 
                                                         _kData.Filename, rowBlockLen);
                }
            }
            else if (kData.Access == EFileAccess.ReadWrite)
            {
                kData.WriteInt64(0, rowBlockLen); // 8
                kData.WriteInt64(KEY_BLOCK_ADDRESS_OFFSET, INITIAL_KEYBLOCK_OFFSET); // 8

                const int defaultKeyBlockSize = 0;
                kData.WriteInt64(INITIAL_KEYBLOCK_OFFSET, defaultKeyBlockSize); // 8
                kData.WriteInt64(INITIAL_KEYBLOCK_OFFSET + 8, 0); // 8
                kData.SetAppendOffset(INITIAL_APPEND_OFFSET);
            }

            _rowBlockSize = rowBlockLen * 8 + 8;
        }

        /// <summary>
        /// Key block length
        /// </summary>
        public int RowBlockSize
        {
            get
            {
                return _rowBlockSize;
            }
        }

        /// <summary>
        /// Number of key records in a key block
        /// </summary>
        public int RowBlockLenBitHint
        {
            get
            {
                return _rowBlockLenBitHint;
            }
        }
        
        public void SaveKeyRecord(KeyRecord rowBlockDet, ITransactionContext tx)
        {
            _kData.WriteInt64(rowBlockDet.KeyOffset, rowBlockDet.BlockOffset);
            _kData.WriteInt64(rowBlockDet.KeyOffset + 8, rowBlockDet.RowCount);
        }

        public long ReadOnlyKeyRecordOffset(int key, PartitionTxData tx)
        {
            // Check transaction.
            var sd = tx.SymbolData[_fileID];
            long keyBlockOffset = sd.KeyBlockOffset;
            CheckKeyBlockOffset(keyBlockOffset);

            // -1 used for null values.
            long keyOffset = keyBlockOffset + KEY_BLOCK_HEADER_SIZE +
                (key + 1) * KEY_RECORD_ENTRY_SIZE;

            if (keyOffset >= keyBlockOffset + KEY_BLOCK_HEADER_SIZE + sd.KeyBlockSize)
            {
                return -1;

            }

            return keyOffset;
        }

        public long ReadKeyRecordOffset(int key, PartitionTxData tx)
        {
            // Check transaction.
            var sd = tx.SymbolData[_fileID];
            if (sd.KeyBlockCreated)
            {
                long keyBlockOffset = sd.KeyBlockOffset;
                long keyOffset = keyBlockOffset + KEY_BLOCK_HEADER_SIZE + (key + 1) * KEY_RECORD_ENTRY_SIZE;
                if (keyOffset > keyBlockOffset + sd.KeyBlockSize)
                {
                    return keyOffset;
                }
            }
            return ReadKeyRecordOffsetSlow(key, tx);
        }

        public long ReadKeyRecordOffsetSlow(int key, PartitionTxData tx)
        {
            var sd = tx.SymbolData[_fileID];
            if (!sd.KeyBlockCreated)
            {
                CopyKeyBlock(tx);
            }

            long keyBlockOffset = sd.KeyBlockOffset;
            CheckKeyBlockOffset(keyBlockOffset);

            // -1 used for null values.
            long keyOffset = keyBlockOffset + KEY_BLOCK_HEADER_SIZE +
                (key + 1) * KEY_RECORD_ENTRY_SIZE;

            if (keyOffset >= keyBlockOffset + sd.KeyBlockSize)
            {
                int newKeyBlockSize = (int)(keyOffset - keyBlockOffset) + KEY_RECORD_ENTRY_SIZE;
                int zerosToWrite = newKeyBlockSize - sd.KeyBlockSize;

                while (zerosToWrite > 0)
                {
                    int sizeBytes = Math.Min(EMPTY_BUFFER_SIZE, zerosToWrite);
                    _kData.WriteBytes(keyBlockOffset + sd.KeyBlockSize, EMPTY_BUFFER, 0, sizeBytes);
                    zerosToWrite -= sizeBytes;
                }

                sd.KeyBlockSize = newKeyBlockSize;
                
            }

            return keyOffset;
        }

        private void CopyKeyBlock(PartitionTxData pd)
        {
            var sd = pd.SymbolData[_fileID];
            var newBlockOffset = pd.AppendOffset[_fileID];
            
            var currentBlockOffset = sd.KeyBlockOffset;
            CheckKeyBlockOffset(currentBlockOffset);

            var currentBlockLen = sd.KeyBlockSize;
            if (currentBlockLen > 0)
            {
                var keyBlockBuff = pd.ReadCache.AllocateCopyKeyBlockArray(currentBlockLen);
                _kData.ReadBytes(currentBlockOffset, keyBlockBuff, 0, currentBlockLen);
                _kData.WriteBytes(newBlockOffset, keyBlockBuff, 0, currentBlockLen);

                sd.KeyBlockOffset = newBlockOffset;
                sd.KeyBlockCreated = true;
            }
        }

        [Conditional("DEBUG")]
        private void CheckKeyBlockOffset(long firstEntryOffset)
        {
            if (firstEntryOffset == 0)
            {
                throw new NFSdbUnsafeDebugCheckException(
                    "First entry offset is not initialized in the transaction for " +
                    "Index Address file " + _kData);
            }
        }
    }
}