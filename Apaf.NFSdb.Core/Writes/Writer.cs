using System;
using System.Threading;
using Apaf.NFSdb.Core.Tx;
using log4net;

namespace Apaf.NFSdb.Core.Writes
{
    public class Writer<T> : IWriter<T>
    {
        private readonly IPartitionManager<T> _partitionManager;
        private readonly object _writeLock;
        private static readonly ILog LOG = LogManager.GetLogger(typeof(Writer<T>));
        private readonly WriterState<T> _writerState;
        private readonly ITransactionContext _transaction;

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
            var timestamp = _writerState.GetTimestampDelegate(item);
            var dateTime = DateUtils.UnixTimestampToDateTime(timestamp);
            var p = _partitionManager.GetAppendPartition(dateTime, _transaction);
            
            p.Append(item, _transaction);
        }

        public void Commit()
        {
            _partitionManager.Commit(_transaction);
        }

        protected void Dispose(bool disposed)
        {
            if (disposed)
            {
                GC.SuppressFinalize(this);
            }
            Monitor.Exit(_writeLock);
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