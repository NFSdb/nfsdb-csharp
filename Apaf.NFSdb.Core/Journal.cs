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
using System.Threading;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core
{
    public class Journal<T> : IJournal<T>
    {
        private readonly IJournalMetadata<T> _metadata;
        private readonly IPartitionManager<T> _partitionManager;
        private readonly object _writeLock = new object();
        private readonly WriterState<T> _writerState;
        private IQueryStatistics _stats;

        public Journal(IJournalMetadata<T> metadata,
            IPartitionManager<T> partitionManager)
        {
            _metadata = metadata;
            _partitionManager = partitionManager;
            _writerState = new WriterState<T>(metadata);
            _stats = new JournalStatistics<T>(_partitionManager, metadata);
            Diagnostics = new JournalDiagnostics(_partitionManager);
        }

        public IJournalMetadata<T> Metadata
        {
            get { return _metadata; }
        }

        public IEnumerable<IPartition<T>> Partitions
        {
            get { return _partitionManager.Partitions.Cast<IPartition<T>>(); }
        }

        public IJournalDiagnostics Diagnostics { get; private set; }

        public IQueryStatistics QueryStatistics { get { return _stats; } }

        public IComparer<long> GetRecordsComparer(int[] columnIndices)
        {
            throw new NotImplementedException();
        }

        public IQuery<T> OpenReadTx()
        {
            var txCntx = _partitionManager.ReadTxLog();
            return new Query<T>(this, txCntx);
        }

        public IWriter<T> OpenWriteTx()
        {
            if (_partitionManager.Access != EFileAccess.ReadWrite)
            {
                throw new InvalidOperationException("Journal is not writable");
            }
            Monitor.Enter(_writeLock);
            return new Writer<T>(_writerState, _partitionManager, _writeLock);
        }

        public T Read(long rowID, IReadContext readContext)
        {
            int partitionIndex = RowIDUtil.ToPartitionIndex(rowID);
            IPartition<T> partition = _partitionManager.GetPartitionByID(partitionIndex);
            return partition.Read(RowIDUtil.ToLocalRowID(rowID), readContext);
        }

        public IEnumerable<T> Read(IEnumerable<long> rowIDs, IReadContext readContext)
        {
            return rowIDs.Select(id => Read(id, readContext));
        }

        public void Dispose()
        {
            _partitionManager.Dispose();
        }

        public IEnumerable<IPartitionCore> PartitionsCore
        {
            get { return _partitionManager.Partitions; }
        }
    }
}