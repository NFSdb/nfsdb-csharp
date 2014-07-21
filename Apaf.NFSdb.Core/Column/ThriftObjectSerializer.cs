#region copyright

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
using System.Linq;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public class ThriftObjectSerializer : IFieldSerializer
    {
        private readonly int _bitsetColSize;

        private readonly Func<ByteArray, IFixedWidthColumn[], long,
            IStringColumn[], IReadContext, object> _fillItemMethod;

        private readonly IFixedWidthColumn[] _fixedColumns;
        private readonly IBitsetColumn _issetColumn;
        private readonly IStringColumn[] _stringColumns;

        private readonly Action<object, ByteArray, IFixedWidthColumn[], long,
            IStringColumn[], ITransactionContext> _writeMethod;

        public ThriftObjectSerializer(IEnumerable<IColumn> columns, 
            Func<ByteArray, IFixedWidthColumn[], long, IStringColumn[], IReadContext, object> readMethod,
            Action<object, ByteArray, IFixedWidthColumn[], long, IStringColumn[], ITransactionContext> writeMethod)
        {
            IColumn[] allColumns = columns.ToArray();

            _fixedColumns = allColumns
                .Where(c => c.FieldType != EFieldType.BitSet
                            && c.FieldType != EFieldType.String
                            && c.FieldType != EFieldType.Symbol)
                .Cast<IFixedWidthColumn>().ToArray();

            _stringColumns = allColumns
                .Where(c => c.FieldType == EFieldType.String
                            || c.FieldType == EFieldType.Symbol)
                .Cast<IStringColumn>().ToArray();

            _issetColumn = (IBitsetColumn)allColumns
                .FirstOrDefault(c => c.FieldType == EFieldType.BitSet);

            if (_issetColumn == null)
            {
                throw new NFSdbInitializationException("Type {0} does not have thrift __isset field defined");
            }
            _fillItemMethod = readMethod;
            _writeMethod = writeMethod;

            _bitsetColSize = _issetColumn.GetByteSize();
        }

        public object Read(long rowID, IReadContext readContext)
        {
            var bitSetAddress = _issetColumn.GetValue(rowID, readContext);
            var byteArray = new ByteArray(bitSetAddress);
            return _fillItemMethod(byteArray, _fixedColumns, rowID, _stringColumns, readContext);
        }

        public void Write(object item, long rowID, ITransactionContext tx)
        {
            var readCache = tx.ReadCache;
            var bitSetAddress = readCache.AllocateByteArray(_bitsetColSize);
            var byteArray = new ByteArray(bitSetAddress);
            _writeMethod(item, byteArray, _fixedColumns, rowID, _stringColumns, tx);
            _issetColumn.SetValue(rowID, bitSetAddress, tx);
        }
    }
}