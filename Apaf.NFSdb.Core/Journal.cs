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
using System.Threading;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core
{
    public class Journal<T> : JournalCore, IJournal<T>
    {
        private readonly IPartitionManager _partitionManager;

        private readonly object _writeLock = new object();
        private readonly WriterState<T> _writerState;

        internal Journal(IJournalMetadata metadata,
            IPartitionManager partitionManager)
            : base(metadata, partitionManager)
        {
            _partitionManager = partitionManager;
            _writerState = new WriterState<T>(metadata.GetTimestampReader<T>());
        }

        public void Truncate()
        {
            throw new NotImplementedException();
        }

        public IQuery<T> OpenReadTx()
        {
            var txCntx = _partitionManager.ReadTxLog(Metadata.PartitionTtl.Milliseconds);
            return new Query<T>(this, txCntx);
        }

        public IWriter OpenWriteTx()
        {
            if (_partitionManager.Access != EFileAccess.ReadWrite)
            {
                throw new InvalidOperationException("Journal is not writable");
            }
            Monitor.Enter(_writeLock);
            return new Writer(_writerState, _partitionManager, _writeLock);
        }

        public IWriter OpenWriteTx(int partitionTtlMs)
        {
            if (_partitionManager.Access != EFileAccess.ReadWrite)
            {
                throw new InvalidOperationException("Journal is not writable");
            }
            Monitor.Enter(_writeLock);
            return new Writer(_writerState, _partitionManager, _writeLock, partitionTtlMs);
        }
    }
}