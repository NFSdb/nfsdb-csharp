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

using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class ResultSet<T> : IEnumerable<T>
    {
        public enum Order
        {
            Asc,
            Desc
        }

        private readonly IEnumerable<long> _rowIDs;
        private readonly IReadTransactionContext _tx;

        internal ResultSet(IEnumerable<long> rowIDs, IReadTransactionContext tx, long? length = null)
        {
            _rowIDs = rowIDs;
            _tx = tx;
            Length = length;
        }

        public long? Length { get; protected set; }

        private IEnumerable<T> Read()
        {
            int lastPartitionID = -1;
            IPartitionReader lastPartitionReader = null;

            foreach (var rowID in _rowIDs)
            {
                int partitionID = RowIDUtil.ToPartitionIndex(rowID);
                if (partitionID != lastPartitionID)
                {
                    lastPartitionReader = _tx.Read(partitionID);
                    lastPartitionID = partitionID;
                }
                long localRowID = RowIDUtil.ToLocalRowID(rowID);
                
                // ReSharper disable once PossibleNullReferenceException
                yield return lastPartitionReader.Read<T>(localRowID, _tx.ReadCache);
            }
        }

        public IEnumerator<T> GetEnumerator()
        {
            return Read().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public ResultSet<T> Reverse()
        {
            return new ResultSet<T>(_rowIDs.Reverse(), _tx, Length);
        }

        public RandomAccessResultSet<T> ToRandomAccess()
        {
            long[] rowIDs = _rowIDs.ToArray();
            return new RandomAccessResultSet<T>(rowIDs, _tx);
        }
    }
}
