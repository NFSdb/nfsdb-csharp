using System;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class PocoObjectSerializer : IFieldSerializer
    {
        private readonly int _bitsetColSize;
        private readonly IFixedWidthColumn[] _fixedColumns;
        private readonly IBitsetColumn _issetColumn;
        private readonly IStringColumn[] _stringColumns;

        private readonly Func<ByteArray, IFixedWidthColumn[], long, IStringColumn[], IReadContext, object> _readMethod;
        private readonly Action<object, ByteArray, IFixedWidthColumn[], long, IStringColumn[], ITransactionContext>
            _writeMethod;

        public PocoObjectSerializer(IEnumerable<IColumn> columns, 
            Func<ByteArray, IFixedWidthColumn[], long, IStringColumn[], IReadContext, object> readMethod,
            Action<object, ByteArray, IFixedWidthColumn[], long, IStringColumn[], ITransactionContext> writeMethod)
        {
            IColumn[] allColumns = columns.ToArray();

            // IFixedWidthColumn array.
            _fixedColumns = allColumns
                        .Where(c => c.FieldType != EFieldType.BitSet
                                    && c.FieldType != EFieldType.String
                                    && c.FieldType != EFieldType.Symbol)
                        .Cast<IFixedWidthColumn>().ToArray();

            // IStringColumn array.
            _stringColumns = allColumns
                .Where(c => c.FieldType == EFieldType.String
                            || c.FieldType == EFieldType.Symbol)
                .Cast<IStringColumn>().ToArray();

            // IBitsetColumn.
            _issetColumn = (IBitsetColumn) allColumns
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
                byte[] bitSetAddress = _issetColumn.GetValue(rowID, readContext);
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
            IReadContext readCache = tx.ReadCache;
            if (_issetColumn != null)
            {
                byte[] bitSetAddress = readCache.AllocateByteArray(_bitsetColSize);
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