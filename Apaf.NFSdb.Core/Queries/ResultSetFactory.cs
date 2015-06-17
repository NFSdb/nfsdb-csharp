using System.Collections.Generic;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class ResultSetFactory
    {
        public static ResultSet<T> Create<T>(IEnumerable<long> rowIDs, IReadTransactionContext tx)
        {
            return new ResultSet<T>(rowIDs, tx);
        }

        public static ResultSet<T> Create<T>(IEnumerable<long> rowIDs, IReadTransactionContext tx, long length)
        {
            return new ResultSet<T>(rowIDs, tx, length);
        }         
    }
}