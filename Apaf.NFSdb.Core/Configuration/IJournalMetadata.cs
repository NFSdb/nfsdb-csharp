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

using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Configuration
{
    public interface IJournalMetadata
    {
        IFieldSerializer GetSerializer(IEnumerable<ColumnSource> columns);
        JournalSettings Settings { get; }
        int? TimestampColumnID { get; }
        int? IsNullColumnID { get; }
        int GetColumnID(string filename);
        int FileCount { get; }
        TimeSpan PartitionTtl { get; }

        IEnumerable<IColumnMetadata> Columns { get; }
        IColumnMetadata GetColumnByID(int columndID);
        IColumnMetadata GetColumnByPropertyName(string symbolName);
        IColumnMetadata TryGetColumnByPropertyName(string symbolName);
        int ColumnCount { get; }
        string Name { get; }

        ColumnSource[] GetPartitionColumns(int paritionID, IColumnStorage partitionStorage, PartitionConfig overrides = null);

        Func<T, DateTime> GetTimestampReader<T>();
    }
}