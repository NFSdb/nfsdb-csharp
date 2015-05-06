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
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Core.Writes;
using log4net;

namespace Apaf.NFSdb.Core.Storage
{
    public class FileTxSupport : IFileTxSupport
    {
        private static readonly ILog LOG = LogManager.GetLogger(typeof(PartitionManagerUtils));
        private readonly int _partitionID;
        private readonly IColumnStorage _storage;
        private readonly IJournalMetadataCore _metadata;
        private const int TIMESTAMP_DATA_SIZE = 8;

        public FileTxSupport(int partitionID, IColumnStorage storage, IJournalMetadataCore metadata)
        {
            _partitionID = partitionID;
            _storage = storage;
            _metadata = metadata;
        }

        public PartitionTxData ReadTxLogFromFile()
        {
            long nextRowID = -1L;
            string lastRowIDFilename = null;
            var pd = new PartitionTxData(_metadata.FileCount, _partitionID);

            foreach (IRawFile file in _storage.AllOpenedFiles())
            {
                ColumnMetadata column;
                long fileAppendOffset;
                try
                {
                    fileAppendOffset = file.GetAppendOffset();
                    pd.AppendOffset[file.FileID] = fileAppendOffset;

                    if (file.DataType == EDataType.Symrk || file.DataType == EDataType.Datak)
                    {
                        var keyBlockOffset = file.ReadInt64(MetadataConstants.K_FILE_KEY_BLOCK_OFFSET);
                        pd.SymbolData[file.FileID].KeyBlockOffset = keyBlockOffset;
                        var keyBlockSize = file.ReadInt64(keyBlockOffset);
                        pd.SymbolData[file.FileID].KeyBlockSize = (int)keyBlockSize;
                    }
                    column = _metadata.GetColumnById(file.ColumnID);
                    if (_metadata.TimestampFieldID == column.FieldID)
                    {
                        var timestamp = file.ReadInt64(fileAppendOffset - TIMESTAMP_DATA_SIZE);
                        pd.LastTimestamp = timestamp;
                    }
                }
                catch (Exception ex)
                {
                    if (ex is NFSdbPartitionException)
                    {
                        throw;
                    }
                    throw new NFSdbTransactionStateExcepton(
                        string.Format("Error reading transaction state from file {0}",
                            file.Filename), ex);
                }
                var size = StorageSizeUtils.GetRecordSize(column, file.DataType);

                if (size > 0)
                {
                    long rowId = fileAppendOffset/size;

                    if (nextRowID >= 0 && rowId != nextRowID)
                    {
                        throw new NFSdbTransactionStateExcepton(
                            string.Format("File {0} has last rowID" +
                                          " different than file {1}. Journal is not consistent" +
                                          " or mutiple writes happened",
                                file.Filename, lastRowIDFilename));
                    }
                    nextRowID = rowId;
                    lastRowIDFilename = file.Filename;
                }
            }
            pd.NextRowID = nextRowID;
            return pd;
        }


        public PartitionTxData ReadTxLogFromPartition(TxRec txRec = null)
        {
            if (txRec == null)
            {
                return ReadTxLogFromFile();
            }
            return ReadTxLogFromFileAndTxRec(txRec);
        }

        private PartitionTxData ReadTxLogFromFileAndTxRec(TxRec txRec)
        {
            int symrRead = 0;
            var pd = new PartitionTxData(_metadata.FileCount, _partitionID);
            long nextRowID = RowIDUtil.ToLocalRowID(txRec.JournalMaxRowID - 1) + 1;
            pd.NextRowID = nextRowID;

            foreach (IRawFile file in _storage.AllOpenedFiles())
            {
                try
                {
                    if (file.DataType == EDataType.Symrk
                        && txRec.SymbolTableIndexPointers != null)
                    {
                        var blockOffset = txRec.SymbolTableIndexPointers[symrRead++];
                        pd.SymbolData[file.FileID].KeyBlockOffset = blockOffset;

                        long keyBlockSize = file.ReadInt64(blockOffset
                                                           + MetadataConstants.K_FILE_ROW_BLOCK_LEN_OFFSET);
                        pd.SymbolData[file.FileID].KeyBlockSize = (int) keyBlockSize;

                        pd.AppendOffset[file.FileID] = blockOffset + keyBlockSize;
                    }
                    else if (file.DataType == EDataType.Datak
                        && txRec.IndexPointers != null)
                    {
                        var blockOffset = txRec.IndexPointers[file.ColumnID];
                        pd.SymbolData[file.FileID].KeyBlockOffset = blockOffset;

                        long keyBlockSize = file.ReadInt64(blockOffset
                                                           + MetadataConstants.K_FILE_ROW_BLOCK_LEN_OFFSET);
                        pd.SymbolData[file.FileID].KeyBlockSize = (int)keyBlockSize;

                        pd.AppendOffset[file.FileID] = blockOffset + keyBlockSize;
                        
                    }
                    else
                    {
                        ColumnMetadata column = _metadata.GetColumnById(file.ColumnID);
                        var size = StorageSizeUtils.GetRecordSize(column, file.DataType);

                        if (size > 0)
                        {
                            // Fixed column.
                            pd.AppendOffset[file.FileID] = nextRowID * size;
                        }
                        else
                        {
                            // Variable column.
                            pd.AppendOffset[file.FileID] = file.GetAppendOffset();
                        }

                        if (_metadata.TimestampFieldID == column.FieldID)
                        {
                            var timestamp = file.ReadInt64(pd.AppendOffset[file.FileID] - TIMESTAMP_DATA_SIZE);
                            pd.LastTimestamp = timestamp;
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (ex is NFSdbPartitionException)
                    {
                        throw;
                    }
                    throw new NFSdbTransactionStateExcepton(
                        string.Format("Error reading transaction state from file {0}",
                            file.Filename), ex);
                }
            }
            pd.NextRowID = nextRowID;
            return pd;
        }

        public IRollback Commit(ITransactionContext newTx)
        {
            var processedFileOffsets = new List<CommitData>(_metadata.FileCount);
            var actionRollaback = new CommitRollback(processedFileOffsets);
            foreach (IRawFile file in _storage.AllOpenedFiles())
            {
                try
                {
                    int partitionID = file.PartitionID;
                    int fileID = file.FileID;
                    var partitionTxData = newTx.GetPartitionTx(partitionID);
                    var oldOffset = file.GetAppendOffset();
                    var appendOffset = partitionTxData.AppendOffset[fileID];

                    if (file.DataType == EDataType.Symrk || file.DataType == EDataType.Datak)
                    {
                        // Key block.
                        var sd = partitionTxData.SymbolData[fileID];
                        var keyBlockOffset = sd.KeyBlockOffset;
                        var keyBlockSize = sd.KeyBlockSize;
                        var oldBlockOffset = file.ReadInt64(MetadataConstants.K_FILE_KEY_BLOCK_OFFSET);
                        file.WriteInt64(MetadataConstants.K_FILE_KEY_BLOCK_OFFSET, keyBlockOffset);
                        file.WriteInt64(keyBlockOffset, keyBlockSize);

                        // Append offset.
                        appendOffset = keyBlockOffset + keyBlockSize;
                        partitionTxData.AppendOffset[fileID] = appendOffset;
                        file.SetAppendOffset(appendOffset);

                        processedFileOffsets.Add(new CommitData(file, oldOffset, oldBlockOffset));
                    }
                    else
                    {
                        // Append offset.
                        file.SetAppendOffset(appendOffset);
                        processedFileOffsets.Add(new CommitData(file, oldOffset));
                    }
                }
                catch (Exception ex)
                {
                    actionRollaback.Rollback();

                    throw new NFSdbCommitFailedException(
                        "SetAppendOffset failed in file {0}", ex, file.Filename);
                }
            }
            return actionRollaback;
        }

        public void SetTxRec(ITransactionContext tx, TxRec rec)
        {
            rec.PrevTxAddress = tx.PrevTxAddress;
            rec.Command = TxRec.TX_NORMAL;
            var pd = tx.GetPartitionTx(_partitionID);

            var columCount = _metadata.Columns.Count();
            if (_partitionID != MetadataConstants.SYMBOL_PARTITION_ID)
            {
                rec.LastPartitionTimestamp = DateUtils.DateTimeToUnixTimeStamp(tx.LastAppendTimestamp);

                // Java NFSdb Journal has paritions from 0 with no reserved id for symbol parition.
                // Max row ID is rowcount + 1 for compatibility
                rec.JournalMaxRowID = RowIDUtil.ToRowID(_partitionID - 1, pd.NextRowID - 1) + 1;

                rec.IndexPointers = new long[columCount];
                foreach (var f in _storage.AllOpenedFiles())
                {
                    if (f.DataType == EDataType.Datak)
                    {
                        rec.IndexPointers[f.ColumnID] = pd.SymbolData[f.FileID].KeyBlockOffset;
                    }
                }
            }
            else
            {
                var symbolTableSize = new List<int>();
                var symbolTableIndexPointers = new List<long>();

                foreach (var file in _storage.AllOpenedFiles())
                {
                    if (file.DataType == EDataType.Symi)
                    {
                        var indexSize = (int) (pd.AppendOffset[file.FileID]/8);
                        symbolTableSize.Add(indexSize);
                    }
                    else if (file.DataType == EDataType.Symrk)
                    {
                        var sd = pd.SymbolData[file.FileID];
                        symbolTableIndexPointers.Add(sd.KeyBlockOffset);
                    }
                }
                rec.SymbolTableIndexPointers = symbolTableIndexPointers.ToArray();
                rec.SymbolTableSizes = symbolTableSize.ToArray();
            }
        }

        private class CommitRollback : IRollback
        {
            private readonly List<CommitData> _processedFiles;

            public CommitRollback(List<CommitData> processedFiles)
            {
                _processedFiles = processedFiles;
            }

            public void Rollback()
            {
                foreach (var rb in _processedFiles)
                {
                    try
                    {
                        rb.File.SetAppendOffset(rb.AppendOffeset);
                        if (rb.IsKeyFile)
                        {
                            rb.File.WriteInt64(MetadataConstants.K_FILE_KEY_BLOCK_OFFSET, rb.KeyBlockOffset);
                        }
                    }
                    catch (Exception ex2)
                    {
                        LOG.ErrorFormat("Error undoing partial commit in file {0}. " +
                                        "Continued, but journal may be in the inconsistent state. {1}",
                            rb.File.Filename, ex2);
                    }
                }
            }
        }

        private class CommitData
        {
            public readonly IRawFile File;
            public readonly long AppendOffeset;
            public readonly long KeyBlockOffset;
            public readonly bool IsKeyFile;

            public CommitData(IRawFile file, 
                long appendOffeset,
                long keyBlockOffset)
            {
                File = file;
                AppendOffeset = appendOffeset;
                KeyBlockOffset = keyBlockOffset;
                IsKeyFile = true;
            }

            public CommitData(IRawFile file, long appendOffeset)
            {
                File = file;
                AppendOffeset = appendOffeset;
            }
        }
    }
}