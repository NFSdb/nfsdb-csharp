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

using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.TestShared
{
    public class PartitionData<T>
    {
        public PartitionData(Partition partition,
            JournalMetadata metadata, ColumnStorage journalStorage,
            string journalPath)
        {
            Partition = partition;
            Metadata = metadata;
            JournalStorage = journalStorage;
            JournalPath = journalPath;
        }

        public string JournalPath { get; set; }

        public ColumnStorage JournalStorage { get; set; }

        public Partition Partition { get; set; }

        public JournalMetadata Metadata { get; set; }
    }
}