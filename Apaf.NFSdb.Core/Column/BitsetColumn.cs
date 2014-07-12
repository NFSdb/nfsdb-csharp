using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public class BitsetColumn : IBitsetColumn
    {
        private readonly IRawFile _storage;
        private readonly int _sizeBytes;
        private static readonly int ISSET_HEADER_LENGTH = MetadataConstants.ISSET_HEADER_LENGTH;
        private readonly int _fieldSize;
        private readonly int _partitionID;
        private readonly int _fileID;

        public BitsetColumn(IRawFile storage, int columCount)
        {
            FieldType = EFieldType.BitSet;
            _storage = storage;
            _fileID = storage.FileID;
            _partitionID = storage.PartitionID;
            
            // we shift the number of fields left 6 bits (divide by 64) to see what the length of the
            // array should be
            _sizeBytes = (((columCount - 1) >> 6) + columCount % 64 == 0 ? 0 : 1) * 8;
            _fieldSize = CalculateSize(columCount);
        }

        public byte[] GetValue(long rowID, IReadContext readContext)
        {
            var byteArray = readContext.AllocateByteArray(_sizeBytes);
            _storage.ReadBytes(rowID * _fieldSize + ISSET_HEADER_LENGTH, 
                byteArray, 0, _sizeBytes);

            return byteArray;
        }

        public void SetValue(long rowID, byte[] bitArray, ITransactionContext tx)
        {
            var offset = rowID * _sizeBytes + ISSET_HEADER_LENGTH;
            _storage.WriteBytes(offset, bitArray, 0, _sizeBytes);
            tx.PartitionTx[_partitionID].AppendOffset[_fileID] = offset + _sizeBytes;
        }

        public int GetByteSize()
        {
            return _fieldSize;
        }

        public static int CalculateSize(int count)
        {
            return (((count - 1) >> 6) + count % 64 == 0 ? 0 : 1) * 8 + ISSET_HEADER_LENGTH;
        }

        public EFieldType FieldType { get; private set; }
        public string PropertyName { get; private set; }
    }
}