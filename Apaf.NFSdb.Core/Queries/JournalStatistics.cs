﻿#region copyright
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

using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class JournalStatistics : IQueryStatistics
    {
        private readonly IJournalMetadata _metadata;
        private readonly IUnsafePartitionManager _partitionManager;

        internal JournalStatistics(IJournalMetadata metadata, IUnsafePartitionManager partitionManager)
        {
            _metadata = metadata;
            _partitionManager = partitionManager;
        }

        public long GetCardinalityByColumnValue<T>(IReadTransactionContext tx, IColumnMetadata col, IList<T> values)
        {
            if (col.Indexed)
            {
                var part = _partitionManager.GetOpenPartitions().FirstOrDefault(p => p != null);
                if (part != null)
                {
                    return values.Sum(value => part.GetSymbolRowCount(col.ColumnID, value, tx));
                }
            }
            return _metadata.Settings.RecordHint / col.HintDistinctCount * values.Count;
        }

        public int GetColumnDistinctCardinality(IReadTransactionContext tx, IColumnMetadata column)
        {
            return column.HintDistinctCount;
        }
    }
}