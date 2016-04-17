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

namespace Apaf.NFSdb.Core.Tx
{
    public interface IReadTransactionContext : IDisposable
    {
        ReadContext ReadCache { get; }
        PartitionTxData GetPartitionTx();
        PartitionTxData GetPartitionTx(int partitionId);
        PartitionTxData SetCurrentPartition(int partitionID);

        long GetRowCount(int partitionID);

        void AddRef(int partitionID);
        void RemoveRef(int paritiotnID);
        void AddRefsAllPartitions();
        void RemoveRefsAllPartitions();

        IPartitionReader Read(int partitionID);
        IEnumerable<IPartitionReader> ReadPartitions { get; }
        IEnumerable<IPartitionReader> ReverseReadPartitions { get; }
        int PartitionCount { get; }
    }
}