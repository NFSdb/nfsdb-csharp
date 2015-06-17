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
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using log4net;

namespace Apaf.NFSdb.Core.Writes
{
    public class Writer<T> : IWriter<T>
    {
        private readonly IPartitionManager<T> _partitionManager;
        private static readonly ILog LOG = LogManager.GetLogger(typeof(Writer<T>));
        private readonly WriterState<T> _writerState;
        private object _writeLock;
        private ITransactionContext _transaction;

        public Writer(WriterState<T> writerState, 
            IPartitionManager<T> partitionManager, object writeLock)
        {
            _writerState = writerState;
            _partitionManager = partitionManager;
            _transaction = _partitionManager.ReadTxLog();
            _writeLock = writeLock;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void Append(T item)
        {
            var dateTime = _writerState.GetTimestampDelegate(item);
            var p = _partitionManager.GetAppendPartition(dateTime, _transaction);
            _transaction.LastAppendTimestamp = dateTime;

            p.Append(item, _transaction);
        }

        public void Commit()
        {
            _partitionManager.Commit(_transaction);
        }

        public void Truncate()
        {
            throw new NotImplementedException();
            _transaction = _partitionManager.ReadTxLog();
        }

        protected void Dispose(bool disposed)
        {
            var lck = _writeLock;
            _writeLock = null;

            if (lck != null)
            {
                if (disposed)
                {
                    GC.SuppressFinalize(this);
                }
                Monitor.Exit(lck);
            }
        }

        ~Writer()
        {
            try
            {
                Dispose(false);
            }
            catch (Exception ex)
            {
                LOG.Error("Error disposing writer in Finalize.", ex);
            }
        }
    }
}