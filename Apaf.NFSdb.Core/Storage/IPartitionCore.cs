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
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IPartitionCore
    {
        int PartitionID { get; }
        string DirectoryPath { get; }
        DateTime StartDate { get; }
        DateTime EndDate { get; }

        IColumnStorage Storage { get; }
        long BinarySearchTimestamp(DateTime value, IReadTransactionContext tx);
        
        IEnumerable<long> GetSymbolRows(string symbol, string value, 
            IReadTransactionContext tx);

        int GetSymbolKey(string symbol, string value, IReadTransactionContext tx);

        IEnumerable<long> GetSymbolRows(string symbol, int valueKey, 
            IReadTransactionContext tx);

        long GetSymbolRowCount(string symbol, string value,
            IReadTransactionContext tx);
    }
}