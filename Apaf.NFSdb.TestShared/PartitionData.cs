#region copyright
/*
 * Copyright (c) 2014. APAF (Alex Pelagenko).
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
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.TestShared
{
    public class PartitionData<T>
    {
        private JournalMetadata<T> _metadata;
        private ColumnStorage _journalStorage;
        private string _journalPath;

        public PartitionData(Partition<T> partition,
            JournalMetadata<T> metadata, ColumnStorage journalStorage,
            string journalPath)
        {
            Partition = partition;
            Metadata = metadata;
            JournalStorage = journalStorage;
            JournalPath = journalPath;
        }

        public string JournalPath
        {
            get { return _journalPath; }
            set { _journalPath = value; }
        }

        public ColumnStorage JournalStorage
        {
            get { return _journalStorage; }
            set { _journalStorage = value; }
        }

        public Partition<T> Partition { get; set; }

        public JournalMetadata<T> Metadata
        {
            get { return _metadata; }
            set { _metadata = value; }
        }


        public ITransactionContext ReadTxLog()
        {
            var tx = new TransactionContext(_metadata.Columns.Count());
            ReadTxLogFromPartition(Partition.PartitionID, Partition.Storage, tx);
            ReadTxLogFromPartition(MetadataConstants.SYMBOL_PARTITION_ID, _journalStorage, tx);
            return tx;
        }

        public void ReadTxLogFromPartition(int partitionID, IColumnStorage files,
            TransactionContext tx)
        {
            throw new NotImplementedException();
        }
    }
}