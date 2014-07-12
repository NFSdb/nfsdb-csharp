using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Core.Writes;
using log4net;

namespace Apaf.NFSdb.Core
{
    public class PartitionManager<T> : IPartitionManager<T>
    {
        // ReSharper disable once StaticFieldInGenericType
        private static readonly ILog LOG = LogManager.GetLogger(typeof (PartitionManagerUtils));
        private const int SYMBOL_PARTITION_ID = MetadataConstants.SYMBOL_PARTITION_ID;
        private readonly ICompositeFileFactory _fileFactory;
        private readonly IJournalMetadata<T> _metadata;
        private readonly List<IPartition<T>> _partitions = new List<IPartition<T>>();
        private readonly JournalSettings _settings;
        private readonly object _lastTransLogSync = new object();
        private readonly ColumnStorage _symbolStorage;
        private readonly FileTxSupport _symbolTxSupport;
        private readonly CompositeRawFile _txLogFile;
        private readonly TxLog _txLog;
        private EPartitionType _partitionType;
        private TransactionContext _lastTransactionLog;
        private readonly int _columnCount;

        public PartitionManager(IJournalMetadata<T> metadata,
            EFileAccess access, ICompositeFileFactory fileFactory)
        {
            Access = access;
            _metadata = metadata;
            _settings = metadata.Settings;
            _fileFactory = fileFactory;

            _symbolStorage = InitializeSymbolStorage();
            _symbolTxSupport = new FileTxSupport(SYMBOL_PARTITION_ID, _symbolStorage, _metadata);
            var txFileName = Path.Combine(metadata.Settings.DefaultPath, MetadataConstants.TX_FILE_NAME);
            _txLogFile = new CompositeRawFile(txFileName,
                MetadataConstants.PIPE_BIT_HINT, _fileFactory, access, SYMBOL_PARTITION_ID,
                MetadataConstants.TX_LOG_FILE_ID, MetadataConstants.TX_LOG_FILE_ID,  EDataType.Data);

            _txLog = new TxLog(_txLogFile);
            Configure();
            _columnCount = _metadata.FileCount;
        }

        public EFileAccess Access { get; private set; }

        public IEnumerable<IPartition<T>> Partitions
        {
            get { return _partitions; }
        }

        public IColumnStorage SymbolFileStorage
        {
            get { return _symbolStorage; }
        }

        public ITxLog TransactionLog { get { return _txLog; } }

        public IPartition<T> GetPartitionByID(int partitionID)
        {
            // Partition IDs are 1 based. 
            // 0 is reserved for symbols "parition".
            return _partitions[partitionID - 1];
        }

        public ITransactionContext ReadTxLog()
        {
            lock (_lastTransLogSync)
            {
                if (_lastTransactionLog != null && Access == EFileAccess.ReadWrite)
                {
                    // This is readwrite instace of Journal.
                    // Any transactions are commited using this instance.
                    return new TransactionContext(_lastTransactionLog);
                }

                // Should be re-written using transaction log.
                if (Access == EFileAccess.Read)
                {
                    DiscoverNewPartitions();
                }

                // _tx file.
                var txRec = _txLog.Get();

                // Check parition count match.
                if (txRec != null)
                {
                    var txRecPartitionID = RowIDUtil.ToPartitionIndex(txRec.JournalMaxRowID);
                    if (txRecPartitionID != _partitions.Count - 1)
                    {
                        txRec = null;
                    }
                }

                var tx = new TransactionContext(_columnCount);
                for (int i = 0; i < _partitions.Count; i++)
                {
                    // Last.
                    _partitions[i].ReadTxLogFromPartition(tx,
                        (i == _partitions.Count - 1) ? txRec : null);
                }
                _symbolTxSupport.ReadTxLogFromPartition(tx, null);

                if (txRec != null)
                {
                    tx.PrevTxAddress = Math.Max(txRec.PrevTxAddress, TxLog.MIN_TX_ADDRESS) + txRec.Size();
                }

                // Set state to inital.
                //

                _lastTransactionLog = new TransactionContext(tx);
                return tx;
            }
        }

        private IEnumerable<IFileTxSupport> AllTxFilesSupport()
        {
            foreach (var partition in _partitions)
            {
                yield return partition;
            }
            yield return _symbolTxSupport;
        }

        public void Commit(ITransactionContext tx)
        {
            if (Access != EFileAccess.ReadWrite)
            {
                throw new NFSdbCommitFailedException(
                    "Journal opened in readonly mode. Transaction commit is not allowed");
            }
            lock (_lastTransLogSync)
            {
                var processedFiles = new Stack<IFileTxSupport>();
                var modified = _partitions.Where(p => tx.IsParitionUpdated(p.PartitionID, _lastTransactionLog));

                // Non-empty tx.
                if (modified.Any())
                {
                    foreach (var txFile in AllTxFilesSupport())
                    {
                        try
                        {
                            txFile.Commit(tx, _lastTransactionLog);
                            processedFiles.Push(txFile);
                        }
                        catch (NFSdbCommitFailedException)
                        {
                            foreach (var rb in processedFiles)
                            {
                                try
                                {
                                    rb.Commit(_lastTransactionLog, _lastTransactionLog);
                                }
                                catch (NFSdbCommitFailedException)
                                {
                                }
                            }
                            throw;
                        }
                    }

                    try
                    {
                        var lastPartition = _partitions[_partitions.Count - 1];
                        var rec = new TxRec();
                        lastPartition.SetTxRec(tx, rec);
                        _symbolTxSupport.SetTxRec(tx, rec);
                        _txLog.Create(rec);
                    }
                    catch (Exception ex)
                    {
                        
                        throw new NFSdbCommitFailedException("Error writing _tx file", ex);
                    }
                }
                _lastTransactionLog = (TransactionContext) tx;
            }
        }

        public IPartition<T> GetAppendPartition(DateTime dateTime, ITransactionContext tx)
        {
            var timestamp = DateUtils.DateTimeToUnixTimeStamp(dateTime);
            if (_partitions.Count > 0)
            {
                for (int i = _partitions.Count - 1; i >= 0; i--)
                {
                    var p = _partitions[i];
                    if (p.IsInsidePartition(dateTime))
                    {
                        long partitionLastTimestamp = 0;

                        // Fully rolled back.
                        if (p.PartitionID >= tx.PartitionTx.Length)
                        {
                            p.ReadTxLogFromPartition(tx, null);
                        }

                        partitionLastTimestamp = tx.PartitionTx[p.PartitionID].LastTimestamp;

                        if (!_metadata.TimestampFieldID.HasValue
                            || timestamp >= partitionLastTimestamp)
                        {
                            return p;
                        }
                        throw new NFSdbInvalidAppendException(
                            "Journal {0}. Attempt to insert a record out of order." +
                            " Record with timestamp {1} cannot be inserted when" +
                            " the last appended record's timestamp is {2}",
                            _metadata.Settings.DefaultPath, 
                            DateUtils.UnixTimestampToDateTime(partitionLastTimestamp),
                            dateTime);
                    }
                    
                    if (i == _partitions.Count - 1 && dateTime > p.StartDate)
                    {
                        break;
                    }
                }
            }

            var startDate = PartitionManagerUtils.GetPartitionStartDate(dateTime,
                _metadata.Settings.PartitionType);

            var dirName = PartitionManagerUtils.GetPartitionDirName(dateTime,
                _metadata.Settings.PartitionType);

            var paritionDir = Path.Combine(_metadata.Settings.DefaultPath, dirName);

            // 0 reserved for symbols.
            var partitionID = _partitions.Count + 1;
            var partition = new Partition<T>(_metadata, _fileFactory, Access, startDate,
                partitionID, paritionDir);    
            partition.ReadTxLogFromPartition(tx, null);
            if (partitionID == 1)
            {
                _symbolTxSupport.ReadTxLogFromFile(tx);
            }
            _partitions.Add(partition);
            
            return partition;
        }

        private ColumnStorage InitializeSymbolStorage()
        {
            var symbolStorage = new ColumnStorage(
                _metadata.Settings, _metadata.Settings.DefaultPath, 
                Access, SYMBOL_PARTITION_ID, _fileFactory);
            _metadata.InitializeSymbols(symbolStorage);
            return symbolStorage;
        }

        private void Configure()
        {
            var di = new DirectoryInfo(_settings.DefaultPath);
            if (!di.Exists)
            {
                di.Create();
            }
            ConfigurePartitionType();
            IEnumerable<string> subDirs = null;

            if (di.Exists)
            {
                subDirs = di.EnumerateDirectories().Select(d => d.Name).OrderBy(s => s);
            }

            _partitionType = _settings.PartitionType;
            if (subDirs != null)
            {
                DebugCheckParitionID();
                int paritionID = SYMBOL_PARTITION_ID + 1;
                
                foreach (string subDir in subDirs.OrderBy(s => s))
                {
                    if (!subDir.StartsWith(MetadataConstants.TEMP_DIRECTORY_PREFIX))
                    {
                        DateTime? startDate = PartitionManagerUtils
                            .ParseDateFromDirName(subDir, _partitionType);

                        if (startDate.HasValue)
                        {
                            _partitions.Add(new Partition<T>(_metadata,
                                _fileFactory, Access, startDate.Value,
                                paritionID++, Path.Combine(di.FullName, subDir)));
                        }
                        else
                        {
                            LOG.ErrorFormat("Invalid directory '{0}' for partition type '{1}'",
                                Path.Combine(di.FullName, subDir), _partitionType);
                        }
                    }
                }
            }
        }

        private void DiscoverNewPartitions()
        {
            var di = new DirectoryInfo(_settings.DefaultPath);
            if (di.Exists)
            {
                var subDirs = di.EnumerateDirectories().Select(d => d.Name).OrderBy(s => s);
                var lastPartitionStart = DateTime.MinValue;
                var lastParitionID = MetadataConstants.SYMBOL_PARTITION_ID + 1;

                if (_partitions.Count > 0)
                {
                    var lastPartition = _partitions[_partitions.Count - 1];
                    lastParitionID = lastPartition.PartitionID + 1;
                    lastPartitionStart = lastPartition.StartDate;
                }

                foreach (string subDir in subDirs.OrderBy(s => s))
                {
                    if (!subDir.StartsWith(MetadataConstants.TEMP_DIRECTORY_PREFIX))
                    {
                        DateTime? startDate = PartitionManagerUtils
                            .ParseDateFromDirName(subDir, _partitionType);

                        if (startDate.HasValue && startDate.Value > lastPartitionStart)
                        {
                            _partitions.Add(new Partition<T>(_metadata,
                                _fileFactory, Access, startDate.Value,
                                lastParitionID++, Path.Combine(di.FullName, subDir)));
                        }
                        else if (!startDate.HasValue)
                        {
                            LOG.ErrorFormat("Invalid directory '{0}' for partition type '{1}'",
                                Path.Combine(di.FullName, subDir), _partitionType);
                        }
                    }
                }
            }
        }

        [Conditional("DEBUG")]
        private void DebugCheckParitionID()
        {
            // ReSharper disable once CSharpWarnings::CS0162
            if (SYMBOL_PARTITION_ID != 0)
            {
                throw new NFSdbConfigurationException("SYMBOL_PARTITION_ID supposed to be 0 but was " + SYMBOL_PARTITION_ID);
            }
        }

        private void ConfigurePartitionType()
        {
            EPartitionType? partitionType = ReadPartitionType();

            if (!partitionType.HasValue)
            {
                partitionType = _settings.PartitionType;
                WritePartitionType(partitionType.Value);
            }
            else
            {
                _settings.OverridePartitionType(partitionType.Value);
            }
        }

        private void WritePartitionType(EPartitionType value)
        {
            string path = Path.Combine(_settings.DefaultPath, MetadataConstants.PARTITION_TYPE_FILENAME);
            File.WriteAllText(path, value.ToString().ToUpper());
        }

        private EPartitionType? ReadPartitionType()
        {
            string path = Path.Combine(_settings.DefaultPath, MetadataConstants.PARTITION_TYPE_FILENAME);
            try
            {
                if (File.Exists(path))
                {
                    EPartitionType val;
                    if (Enum.TryParse(File.ReadAllText(path), true, out val))
                    {
                        return val;
                    }
                }
            }
            catch (IOException)
            {
            }
            return null;
        }

        public void Dispose()
        {
            _txLogFile.Dispose();
            foreach (var partition in _partitions)
            {
                partition.Dispose();
            }
            _partitions.Clear();

            _symbolStorage.Dispose();
        }
    }
}