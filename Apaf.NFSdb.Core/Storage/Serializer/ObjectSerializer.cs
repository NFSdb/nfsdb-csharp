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
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class ObjectSerializer : IFieldSerializer
    {
        private readonly int _bitsetColSize;

        private readonly Func<ByteArray, IFixedWidthColumn[], long,
            IRefTypeColumn[], IReadContext, object> _fillItemMethod;

        private readonly IFixedWidthColumn[] _fixedColumns;
        private readonly IBitsetColumn _issetColumn;
        private readonly IRefTypeColumn[] _refTypeColumns;

        private readonly Action<object, ByteArray, IFixedWidthColumn[], long,
            IRefTypeColumn[], ITransactionContext> _writeMethod;

        public ObjectSerializer(IEnumerable<ColumnSource> columns, 
            Func<ByteArray, IFixedWidthColumn[], long, IRefTypeColumn[], IReadContext, object> readMethod,
            Action<object, ByteArray, IFixedWidthColumn[], long, IRefTypeColumn[], ITransactionContext> writeMethod)
        {
            var allColumns = columns.ToArray();
            _fixedColumns = allColumns
                .Where(c => !((IClassColumnSerializerMetadata)c.Metadata.SerializerMetadata).IsRefType() && c.Metadata.SerializerMetadata.ColumnType != EFieldType.BitSet)
                .Select(c => c.Column)
                .Cast<IFixedWidthColumn>().ToArray();

            _refTypeColumns = allColumns
                .Where(c => ((IClassColumnSerializerMetadata)c.Metadata.SerializerMetadata).IsRefType())
                .Select(c => c.Column)
                .Cast<IRefTypeColumn>().ToArray();

            _issetColumn = (IBitsetColumn)allColumns
                .Select(c => c.Column)
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
            return _fillItemMethod(byteArray, _fixedColumns, rowID, _refTypeColumns, readContext);
        }

        public void Write(object item, long rowID, ITransactionContext tx)
        {
            var readCache = tx.ReadCache;
            var bitSetAddress = readCache.AllocateByteArray(_bitsetColSize);
            var byteArray = new ByteArray(bitSetAddress);
            _writeMethod(item, byteArray, _fixedColumns, rowID, _refTypeColumns, tx);
            _issetColumn.SetValue(rowID, bitSetAddress, tx);
        }
    }
}