﻿using System.Collections.Generic;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Queries
{
    public class ResultSetFactory
    {
        public static ResultSet<T> Create<T>(IJournal<T> journal, IReadContext readContext, IEnumerable<long> rowIDs, ITxPartitionLock transaction)
        {
            return new ResultSet<T>(journal, readContext, rowIDs, transaction);
        }

        public static ResultSet<T> Create<T>(IJournal<T> journal, IReadContext readContext, IEnumerable<long> rowIDs, long length, ITxPartitionLock transaction)
        {
            return new ResultSet<T>(journal, readContext, rowIDs, transaction, length);
        }         
    }
}