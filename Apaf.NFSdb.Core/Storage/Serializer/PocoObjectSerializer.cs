using System;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class PocoObjectSerializer : IFieldSerializer
    {
        private readonly Func<ByteArray, IFixedWidthColumn[], long, IStringColumn[], IReadContext, object> _readMethod;
        private readonly Action<object, ByteArray, IFixedWidthColumn[], long, IStringColumn[], ITransactionContext> _writeMethod;
        private readonly IFixedWidthColumn[] _fixedColumns;
        private readonly FixedColumnNullableWrapper[] _nullableFixedColumns;
        private readonly IStringColumn[] _stringColumns;
        private readonly IBitsetColumn _issetColumn;
        private readonly int _bitsetColSize;

        public PocoObjectSerializer(IEnumerable<IColumn> columns, FieldData[] allDataColumns, 
            Func<ByteArray, IFixedWidthColumn[], long, IStringColumn[], IReadContext, object> readMethod, 
            Action<object, ByteArray, IFixedWidthColumn[], long, IStringColumn[], ITransactionContext> writeMethod)
        {
            var allColumns = columns.ToArray();

            // IFixedWidthColumn array.
            _fixedColumns = new IFixedWidthColumn[allDataColumns.Length];

            // FixedColumnNullableWrapper array.
            _nullableFixedColumns = new FixedColumnNullableWrapper[allColumns.Length];
            int bitIndex = 0;
            int fci = 0;
            for (int i = 0; i < allDataColumns.Length; i++)
            {
                var field = allDataColumns[i];
                var column = allColumns[i];
                //if (field.Nulllable)
                //{
                //    _nullableFixedColumns[bitIndex] = new FixedColumnNullableWrapper(
                //        (IFixedWidthColumn)column, bitIndex);
                //    bitIndex++;
                //}
                //else 
                if (field.DataType != EFieldType.BitSet
                            && field.DataType != EFieldType.String
                            && field.DataType != EFieldType.Symbol)
                {
                    _fixedColumns[fci++] = (IFixedWidthColumn)column;
                }
            }

            // IStringColumn array.
            _stringColumns = allColumns
                .Where(c => c.FieldType == EFieldType.String
                            || c.FieldType == EFieldType.Symbol)
                .Cast<IStringColumn>().ToArray();

            // IBitsetColumn.
            _issetColumn = (IBitsetColumn)allColumns
                .FirstOrDefault(c => c.FieldType == EFieldType.BitSet);

            if (_issetColumn != null)
            {
                _bitsetColSize = _issetColumn.GetByteSize();
            }

            // Read - write generated methods.
            _readMethod = readMethod;
            _writeMethod = writeMethod;
        }

        public object Read(long rowID, IReadContext readContext)
        {
            if (_issetColumn != null)
            {
                var bitSetAddress = _issetColumn.GetValue(rowID, readContext);
                var byteArray = new ByteArray(bitSetAddress);
                return _readMethod(byteArray, _fixedColumns, rowID, _stringColumns, readContext);
            }
            else
            {
                var byteArray = new ByteArray();
                return _readMethod(byteArray, _fixedColumns, rowID, _stringColumns, readContext);
            }
        }

        public void Write(object item, long rowID, ITransactionContext tx)
        {
            var readCache = tx.ReadCache;
            if (_issetColumn != null)
            {
                var bitSetAddress = readCache.AllocateByteArray(_bitsetColSize);
                var byteArray = new ByteArray(bitSetAddress);
                _writeMethod(item, byteArray, _fixedColumns, rowID, _stringColumns, tx);
                _issetColumn.SetValue(rowID, bitSetAddress, tx);
            }
            else
            {
                var byteArray = new ByteArray();
                _writeMethod(item, byteArray, _fixedColumns, rowID, _stringColumns, tx);
            }
        }
    }
}