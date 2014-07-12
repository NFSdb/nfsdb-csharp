using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public class FixedColumn : IFixedWidthColumn
    {
        private readonly EFieldType _fieldType;
        private readonly int _sizeBytes;
        private readonly IRawFile _storage;
        private readonly int _partitionID;
        private readonly int _fileID;

        public FixedColumn(IRawFile storage, EFieldType fieldType, string propertyName = null)
        {
            _storage = storage;
            _fieldType = fieldType;
            _sizeBytes = fieldType.GetSize();
            _partitionID = storage.PartitionID;
            _fileID = storage.FileID;
            PropertyName = propertyName;
        }

        public int GetInt32(long rowID)
        {
            return _storage.ReadInt32(rowID * _sizeBytes);
        }

        public long GetInt64(long rowID)
        {
            return _storage.ReadInt64(rowID * _sizeBytes);
        }

        public short GetInt16(long rowID)
        {
            return _storage.ReadInt16(rowID * _sizeBytes);
        }

        public byte GetByte(long rowID)
        {
            return _storage.ReadByte(rowID * _sizeBytes);
        }

        public bool GetBool(long rowID)
        {
            return _storage.ReadBool(rowID * _sizeBytes);
        }

        public double GetDouble(long rowID)
        {
            return _storage.ReadDouble(rowID * _sizeBytes);
        }

        public void SetInt32(long rowID, int value, ITransactionContext tx)
        {
            var offset = rowID*_sizeBytes;
            _storage.WriteInt32(offset, value);
            tx.PartitionTx[_partitionID].AppendOffset[_fileID] = offset + _sizeBytes;
        }

        public void SetInt64(long rowID, long value, ITransactionContext tx)
        {
            var offset = rowID * _sizeBytes;
            _storage.WriteInt64(offset, value);
            tx.PartitionTx[_partitionID].AppendOffset[_fileID] = offset + _sizeBytes;
        }

        public void SetInt16(long rowID, short value, ITransactionContext tx)
        {
            var offset = rowID * _sizeBytes;
            _storage.WriteInt16(offset, value);
            tx.PartitionTx[_partitionID].AppendOffset[_fileID] = offset + _sizeBytes;
        }

        public void SetByte(long rowID, byte value, ITransactionContext tx)
        {
            var offset = rowID * _sizeBytes;
            _storage.WriteByte(offset, value);
            tx.PartitionTx[_partitionID].AppendOffset[_fileID] = offset + _sizeBytes;
        }

        public void SetBool(long rowID, bool value, ITransactionContext tx)
        {
            var offset = rowID * _sizeBytes;
            _storage.WriteBool(offset, value);
            tx.PartitionTx[_partitionID].AppendOffset[_fileID] = offset + _sizeBytes;
        }

        public void SetDouble(long rowID, double value, ITransactionContext tx)
        {
            var offset = rowID * _sizeBytes;
            _storage.WriteDouble(offset, value);
            tx.PartitionTx[_partitionID].AppendOffset[_fileID] = offset + _sizeBytes;
        }

        public EFieldType FieldType
        {
            get { return _fieldType; }
        }

        public string PropertyName { get; private set; }
    }
}