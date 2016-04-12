using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class FixedColumnNullableWrapper
    {
        private readonly IFixedWidthColumn _column;
        private readonly int _bitsetIndex;

        public FixedColumnNullableWrapper(IFixedWidthColumn column, int bitsetIndex)
        {
            _column = column;
            _bitsetIndex = bitsetIndex;
        }

        public void SetNullableInt32(long rowID, int? value, ByteArray isNull, ITransactionContext readContext)
        {
            if (value.HasValue)
            {
                _column.SetInt32(rowID, value.GetValueOrDefault());
            }
            isNull.Set(_bitsetIndex, !value.HasValue);
        }

        public int? GetNullableInt32(long rowid, ByteArray bitset)
        {
            if (!bitset.IsSet(_bitsetIndex))
            {
                return _column.GetInt32(rowid);
            }
            return default(int?);
        }

        public void SetNullableInt64(long rowID, long? value, ByteArray isNull, ITransactionContext readContext)
        {
            if (value.HasValue)
            {
                _column.SetInt64(rowID, value.GetValueOrDefault());
            }
            isNull.Set(_bitsetIndex, !value.HasValue);
        }

        public long? GetNullableInt64(long rowid, ByteArray bitset)
        {
            if (!bitset.IsSet(_bitsetIndex))
            {
                return _column.GetInt64(rowid);
            }
            return default(long?);
        }

        public void SetNullableInt16(long rowID, short? value, ByteArray isNull, ITransactionContext readContext)
        {
            if (value.HasValue)
            {
                _column.SetInt16(rowID, value.GetValueOrDefault());
            }
            isNull.Set(_bitsetIndex, !value.HasValue);
        }

        public short? GetNullableInt16(long rowid, ByteArray bitset)
        {
            if (!bitset.IsSet(_bitsetIndex))
            {
                return _column.GetInt16(rowid);
            }
            return default(short?);
        }

        public void SetNullableByte(long rowID, byte? value, ByteArray isNull, ITransactionContext readContext)
        {
            if (value.HasValue)
            {
                _column.SetByte(rowID, value.GetValueOrDefault());
            }
            isNull.Set(_bitsetIndex, !value.HasValue);
        }

        public byte? GetNullableByte(long rowid, ByteArray bitset)
        {
            if (!bitset.IsSet(_bitsetIndex))
            {
                return _column.GetByte(rowid);
            }
            return default(byte?);
        }

        public void SetNullableBool(long rowID, bool? value, ByteArray isNull, ITransactionContext readContext)
        {
            if (value.HasValue)
            {
                _column.SetBool(rowID, value.GetValueOrDefault());
            }
            isNull.Set(_bitsetIndex, !value.HasValue);
        }

        public bool? GetNullableBool(long rowid, ByteArray bitset)
        {
            if (!bitset.IsSet(_bitsetIndex))
            {
                return _column.GetBool(rowid);
            }
            return default(bool?);
        }

        public void SetNullableDouble(long rowID, double? value, ByteArray isNull, ITransactionContext readContext)
        {
            if (value.HasValue)
            {
                _column.SetDouble(rowID, value.GetValueOrDefault());
            }
            isNull.Set(_bitsetIndex, !value.HasValue);
        }

        public double? GetNullableDouble(long rowid, ByteArray bitset)
        {
            if (!bitset.IsSet(_bitsetIndex))
            {
                return  new double?(_column.GetDouble(rowid));
//                return 1.0;
            }
            return default(double?);
        }
    }
}