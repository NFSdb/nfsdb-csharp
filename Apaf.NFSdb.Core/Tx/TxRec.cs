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
namespace Apaf.NFSdb.Core.Tx
{
    public class TxRec
    {
        public static readonly byte TX_NORMAL = 0;

        // transient
        public long Address;
        // 8
        public long PrevTxAddress;
        // 1
        public byte Command;
        // 8
        public long Timestamp;
        // 8
        public long JournalMaxRowID;
        // 8
        public long LastPartitionTimestamp;
        // 8
        public long LagSize;
        // 1 + 1 + 64
        public string LagName;
        // 2 + 4 * symbolTableSizes.len
        public int[] SymbolTableSizes;
        // 2 + 8 * symbolTableIndexPointers.len
        public long[] SymbolTableIndexPointers;
        // 2 + 8 * indexPointers.len
        public long[] IndexPointers;
        // 2 + 8 * lagIndexPointers.len
        public long[] LagIndexPointers;

        public int Size()
        {
            return 8 + 1 + 8 + 8 + 8 + 8
                   + 1 + 1 + 64
                   + 2 + 4*(SymbolTableSizes == null ? 0 : SymbolTableSizes.Length)
                   + 2 + 8*(SymbolTableIndexPointers == null ? 0 : SymbolTableIndexPointers.Length)
                   + 2 + 8*(IndexPointers == null ? 0 : IndexPointers.Length)
                   + 2 + 8*(LagIndexPointers == null ? 0 : LagIndexPointers.Length);
        }

        public override string ToString()
        {
            return "TxRec{" +
                   "address=" + Address +
                   ", prevTxAddress=" + PrevTxAddress +
                   ", command=" + Command +
                   ", timestamp=" + Timestamp +
                   ", journalMaxRowID=" + JournalMaxRowID +
                   ", lastPartitionTimestamp=" + LastPartitionTimestamp +
                   ", lagSize=" + LagSize +
                   ", lagName='" + LagName + '\'' +
                   ", symbolTableSizes=" + SymbolTableSizes +
                   ", symbolTableIndexPointers=" + SymbolTableIndexPointers +
                   ", indexPointers=" + IndexPointers +
                   ", lagIndexPointers=" + LagIndexPointers +
                   ", size= " + Size() + "}";
        }
    }
}