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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Server;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core
{
    public class Journal<T> : JournalCore, IJournal<T>
    {
        private readonly IPartitionManager _partitionManager;
        private readonly IJournalServer _server;
        private readonly object _writeLock = new object();
        private readonly WriterState<T> _writerState;
        private Func<IQueryable<T>, IEnumerable<T>> _compressor;
        private readonly IJournalMetadata _metadata;

        internal Journal(IJournalMetadata metadata,
            IPartitionManager partitionManager, IJournalServer server)
            : base(metadata, partitionManager)
        {
            _metadata = metadata;
            _partitionManager = partitionManager;
            _server = server;
            _partitionManager.OnCommited += OnCommited;
            _writerState = new WriterState<T>(metadata.GetTimestampReader<T>());
        }

        private void OnCommited(long recordIdFrom, long recordIdTo)
        {
            var compressor = _compressor;
            if (compressor != null)
            {
                var start = RowIDUtil.ToPartitionIDFromExternalRowID(recordIdFrom);
                var end = RowIDUtil.ToPartitionIDFromExternalRowID(recordIdTo);

                for (var id = start; id < end; id++)
                {
                    _server.Execute(() => CompressPartition(start, compressor), "Compress partition", 0);
                }
            }
        }

        private void CompressPartition(int partitionID, Func<IQueryable<T>, IEnumerable<T>> compressor)
        {
            using (var txCntx = _partitionManager.ReadTxLog(Metadata.PartitionTtl.Milliseconds))
            {
                var tx = new Query<T>(this, txCntx);
                var date = txCntx.Partitions[partitionID].StartDate;
                var version = txCntx.Partitions[partitionID].Version;
                var resultSet = tx.Items.ByPartitionId(partitionID);
                var tempPartition = _partitionManager.CreateTempPartition(partitionID, date, version);
                var newTx = new PartitionTxData(_metadata.ColumnCount, partitionID, new ReadContext());

                try
                {
                    foreach (var record in compressor(resultSet))
                    {
                        tempPartition.Append(record, newTx);
                    }
                }
                catch (Exception ex)
                {
                    Trace.TraceError("Failed to compress partition", ex);
                    _partitionManager.RemoveTempPartition(tempPartition);
                }

                _partitionManager.CommitTempPartition(tempPartition, newTx);
            }
        }

        public void Truncate()
        {
            throw new NotImplementedException();
        }

        public void SetCompression(Func<IQueryable<T>, IEnumerable<T>> compressor)
        {
            _compressor = compressor;
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