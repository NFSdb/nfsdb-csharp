using System;
using System.Diagnostics;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    // File binary structure
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

        private readonly int _rowBlockLen;
        private readonly int _rowBlockSize;

        public IndexAddressColumn(IRawFile kData, int capacity, int recordCountHint)
        {
            _fileID = kData.FileID;
            _partitionID = kData.PartitionID;
            int keycountHint = Math.Max(capacity, 1);

            // Nuber of records per keyblock
            // is a half of statistically expected
            // but minimum size is 10
            _rowBlockLen = Math.Max(recordCountHint / keycountHint / 2, 10);
            _kData = kData;


            if (kData.GetAppendOffset() > 0)
            {
                _rowBlockLen = _kData.ReadInt32(ROW_BLOK_LEN_ADDRESS_OFFSET);
            }
            else if (kData.Access == EFileAccess.ReadWrite)
            {
                kData.WriteInt64(0, _rowBlockLen); // 8
                kData.WriteInt64(KEY_BLOCK_ADDRESS_OFFSET, INITIAL_KEYBLOCK_OFFSET); // 8

                const int defaultKeyBlockSize = 0;
                kData.WriteInt64(INITIAL_KEYBLOCK_OFFSET, defaultKeyBlockSize); // 8
                kData.WriteInt64(INITIAL_KEYBLOCK_OFFSET + 8, 0); // 8
                kData.SetAppendOffset(INITIAL_APPEND_OFFSET);
            }

            _rowBlockSize = _rowBlockLen * 8 + 8;
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
        public int RowBlockLen
        {
            get
            {
                return _rowBlockLen;
            }
        }
        
        public void SaveKeyRecord(KeyRecord rowBlockDet, ITransactionContext tx)
        {
            _kData.WriteInt64(rowBlockDet.KeyOffset, rowBlockDet.BlockOffset);
            _kData.WriteInt64(rowBlockDet.KeyOffset + 8, rowBlockDet.RowCount);
        }
        
        public long ReadOnlyKeyRecordOffset(int key, IReadTransactionContext tx)
        {
            // Check transaction.
            var sd = tx.PartitionTx[_partitionID].SymbolData[_fileID];
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

        public long ReadKeyRecordOffset(int key, ITransactionContext tx)
        {
            // Check transaction.
            var sd = tx.PartitionTx[_partitionID].SymbolData[_fileID];
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

        private void CopyKeyBlock(ITransactionContext tx)
        {
            var sd = tx.PartitionTx[_partitionID].SymbolData[_fileID];
            var newBlockOffset = tx.PartitionTx[_partitionID].AppendOffset[_fileID];
            
            var currentBlockOffset = sd.KeyBlockOffset;
            CheckKeyBlockOffset(currentBlockOffset);
         
            var currentBlockLen = (int) sd.KeyBlockSize;
            var keyBlockBuff = tx.ReadCache.AllocateByteArray3(currentBlockLen);
            _kData.ReadBytes(currentBlockOffset, keyBlockBuff, 0, keyBlockBuff.Length);
            _kData.WriteBytes(newBlockOffset, keyBlockBuff, 0, currentBlockLen);

            sd.KeyBlockOffset = newBlockOffset;
            sd.KeyBlockCreated = true;
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