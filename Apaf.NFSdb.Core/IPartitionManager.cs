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
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core
{
    public interface IPartitionManager<T> : IDisposable
    {
        EFileAccess Access { get; }
        IEnumerable<IPartition<T>> Partitions { get; }
        IColumnStorage SymbolFileStorage { get; }
        ITxLog TransactionLog { get; }

        IPartition<T> GetPartitionByID(int partitionID);
        ITransactionContext ReadTxLog();
        IPartition<T> GetAppendPartition(DateTime dateTime, ITransactionContext tx);
        void Commit(ITransactionContext transaction);
    }
}