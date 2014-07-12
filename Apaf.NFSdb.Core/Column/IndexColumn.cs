using System.Collections.Generic;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    // File binary structure
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

        public IndexColumn(IRawFile kData, IRawFile rData, int capacity, int recordCountHint)
        {
            _rFileID = rData.FileID;
            _kData = kData;
            _indexAddress = new IndexAddressColumn(kData, capacity, recordCountHint);
            _rowBlockLen = _indexAddress.RowBlockLen;
            _rowBlockSize = _indexAddress.RowBlockSize;
            _rData = rData;
        }

        public IEnumerable<long> GetValues(int hashKey, IReadTransactionContext tx)
        {
            var keyRecOffset = _indexAddress.ReadOnlyKeyRecordOffset(hashKey, tx);
            if (keyRecOffset < 0)
            {
                yield break;
            }
            var blockOffset = _kData.ReadInt64(keyRecOffset);
            var rowCount = _kData.ReadInt64(keyRecOffset + 8);

            // Key found.
            var rowBlockLen = _indexAddress.RowBlockLen;
            var rowBlockSize = _indexAddress.RowBlockSize;
            var rowBlockCount = (int)(rowCount / rowBlockLen) + 1;
            var len = (int)(rowCount % rowBlockLen);
            if (len == 0)
            {
                rowBlockCount--;
                len = rowBlockLen;
            }

            var rowBlockOffset = blockOffset;

            // Loop blocks.
            for (int i = rowBlockCount - 1; i >=0 ; i--)
            {
                // Loop cells.
                for (int k = len - 1; k >=0; k--)
                {
                    yield return _rData.ReadInt64(rowBlockOffset - rowBlockSize + k * 8);
                }

                // Next block.
                rowBlockOffset = _rData.ReadInt64(rowBlockOffset - 8);
                len = rowBlockLen;
            }
        }

        public long GetCount(int valueKey, IReadTransactionContext tx)
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

        public void Add(int key, long value, ITransactionContext tx)
        {
            var keyRecOffset = _indexAddress.ReadKeyRecordOffset(key, tx);
            var blockOffset = _kData.ReadInt64(keyRecOffset);
            long offset = keyRecOffset + 8;
            var rowCount = _kData.ReadInt64(offset);
            var cellIndex = (int)(rowCount % _rowBlockLen);
            
            if (blockOffset == 0 || cellIndex == 0)
            {
                var prevRowBlockOffset = blockOffset;
                blockOffset = tx.PartitionTx[_rData.PartitionID].AppendOffset[_rFileID] + _rowBlockSize;
                tx.PartitionTx[_rData.PartitionID].AppendOffset[_rFileID] = blockOffset;
                _rData.WriteInt64(blockOffset - 8, prevRowBlockOffset);
                
                // Save block offset in k file
                _kData.WriteInt64(keyRecOffset, blockOffset);
            }

            // Save value in index
            _rData.WriteInt64(blockOffset - _rowBlockSize + 8 * cellIndex, value);
            // Save row count in k file
            _kData.WriteInt64(offset, ++rowCount);
        }
    }
}