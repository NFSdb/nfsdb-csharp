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
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class JournalStatistics : IQueryStatistics
    {
        private readonly IJournalMetadataCore _metadata;
        private readonly IUnsafePartitionManager _partitionManager;

        internal JournalStatistics(IJournalMetadataCore metadata, IUnsafePartitionManager partitionManager)
        {
            _metadata = metadata;
            _partitionManager = partitionManager;
        }

        public long GetCardinalityByColumnValue<T>(IReadTransactionContext tx, ColumnMetadata col, T[] values)
        {
            if (col.Indexed)
            {
                var part = _partitionManager.GetOpenPartitions().FirstOrDefault(p => p != null);
                if (part != null)
                {
                    return values.Sum(value => part.GetSymbolRowCount(col.FieldID, value, tx));
                }
            }
            return _metadata.Settings.RecordHint / col.HintDistinctCount * values.Length;
        }

        public int GetColumnDistinctCardinality(IReadTransactionContext tx, ColumnMetadata column)
        {
            if (column.Indexed)
            {
                return GetSymbolCount(tx, column);
            }
            return column.HintDistinctCount;
        }

        public int GetSymbolCount(IReadTransactionContext tx, ColumnMetadata column)
        {
            var storage = _partitionManager.SymbolFileStorage;

            IRawFile symiFile = null;
            if (column.Indexed)
            {
                for (int i = 0; i < storage.OpenFileCount; i++)
                {
                    IRawFile file = storage.GetOpenedFileByID(i);
                    if (file != null && file.ColumnID == column.FieldID
                        && file.DataType == EDataType.Symi)
                    {
                        symiFile = file;
                        break;
                    }
                }

                if (symiFile == null)
                {
                    return 0;
                }

                return (int)(
                    tx.GetPartitionTx(MetadataConstants.SYMBOL_PARTITION_ID).AppendOffset[symiFile.FileID]
                    / MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE);
            }
            throw new NFSdbQuaryableNotSupportedException("Column {0} is not indexed and presice distinct count is not available",
                column.PropertyName);
        }
    }
}