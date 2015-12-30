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
using Apaf.NFSdb.Core.Storage.Serializer.Records;

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class RecordSerializerFactory : ISerializerFactory
    {
        private readonly JournalElement _config;

        public RecordSerializerFactory(JournalElement config)
        {
            _config = config;
        }

        public IEnumerable<IColumnSerializerMetadata> Initialize(Type objectType)
        {
            int i = 0;
            var allColumns = _config.Columns.Select(c => new RecordSerializerMetadata(
                c.ColumnType, c.Name, c.IsNull, i++)).ToList();

            var nullColsCount = allColumns.Count(c => c.Nullable);
            if (nullColsCount > 0)
            {
                // Add bitset.
                return allColumns.Concat(new[]
                {
                    new RecordSerializerMetadata(EFieldType.BitSet, MetadataConstants.NULLS_FILE_NAME, false,
                        i, nullColsCount)
                });
            }
            return allColumns;
        }

        public IFieldSerializer CreateFieldSerializer(IEnumerable<ColumnSource> columns)
        {
            // TODO
            return null;
        }

        public Func<T, TRes> ColumnReader<T, TRes>(IColumnSerializerMetadata column)
        {
            if (typeof (T) != typeof (Record))
            {
                throw new NFSdbInvalidReadException("Record serializer is used on object of type " + typeof(T));
            }
            return (Func<T, TRes>) ((object) (new Func<Record, TRes>(RecordColumnReader<TRes>(column))));
        }

        
        public Func<Record, TRes> RecordColumnReader<TRes>(IColumnSerializerMetadata column)
        {
            var columnId = ((RecordSerializerMetadata) column).ColumnId;
            return rec => rec.RecordSet.Get<TRes>(rec.RowId, columnId);
        }
    }
}