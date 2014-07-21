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
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class JournalStatistics<T> : IQueryStatistics
    {
        private readonly IPartitionManager<T> _partitionManager;
        private readonly IJournalMetadata<T> _metadata;

        public JournalStatistics(IPartitionManager<T> partitionManager, IJournalMetadata<T> metadata)
        {
            _partitionManager = partitionManager;
            _metadata = metadata;
        }

        public long RowsBySymbolValue(IReadTransactionContext tx, string symbolName, string[] values)
        {
            return _partitionManager.Partitions.Sum(
                part => values.Sum(value => part.GetSymbolRowCount(symbolName, value, tx)));
        }

        public int GetSymbolCount(IReadTransactionContext tx, string symbolName)
        {
            var column = _metadata.Columns.Single(c => c.PropertyName == symbolName);

            var symiFile = _partitionManager.SymbolFileStorage.AllOpenedFiles()
                .Single(f => f.ColumnID == column.FieldID && f.DataType == EDataType.Symi);
            return (int) (
                tx.PartitionTx[MetadataConstants.SYMBOL_PARTITION_ID].AppendOffset[symiFile.FileID] 
                / MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE);
        }
    }
}