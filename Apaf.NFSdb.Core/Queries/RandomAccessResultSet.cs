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
using System.Collections.Generic;
using Apaf.NFSdb.Core.Collections;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    internal static class RandomAccessResultSetInternal
    {
        public static readonly long[] EMPTY_IDS = new long[0];
        public static readonly IReadContext NON_SHARED_READ_CONTEXT = new MutithreadedReadContext();

        internal class MutithreadedReadContext : IReadContext
        {
            public byte[] AllocateByteArray(int size)
            {
                return new byte[size];
            }

            public byte[] AllocateByteArray2(int size)
            {
                return new byte[size];
            }

            public byte[] AllocateByteArray3(int size)
            {
                return new byte[size];
            }

            public IObjIntHashMap ColumnNames
            {
                get
                {
                    // This should not be practically used in multithreaded result set.
                    return new ObjIntHashMap();
                }
            }
        }
    }

    public class RandomAccessResultSet<T> : ResultSet<T>
    {
        private readonly IList<long> _idArray;
        private readonly IReadTransactionContext _tx;

        public RandomAccessResultSet(IList<long> rowIDs, IReadTransactionContext tx)
            : base(rowIDs, tx, rowIDs.Count)
        {
            _idArray = rowIDs;
            _tx = tx;
            Length = rowIDs.Count;
        }

        public RandomAccessResultSet() : base(RandomAccessResultSetInternal.EMPTY_IDS, null, 0)
        {
            _idArray = RandomAccessResultSetInternal.EMPTY_IDS;
            Length = 0;
        }

        public IList<long> GetRowIDs()
        {
            return _idArray;
        }

        public T Read(int rsIndex)
        {
            long rowID = _idArray[rsIndex];
            int partitionID = RowIDUtil.ToPartitionIndex(rowID);
            long localRowID = RowIDUtil.ToLocalRowID(rowID);
            return _tx.Read(partitionID).Read<T>(localRowID, RandomAccessResultSetInternal.NON_SHARED_READ_CONTEXT);
        }

        public long GetRowID(int index)
        {
            return _idArray[index];
        }
    }
}