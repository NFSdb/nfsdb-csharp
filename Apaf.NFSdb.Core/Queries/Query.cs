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
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class Query<T> : IQuery<T>
    {
        private IReadTransactionContext _transactionContext;
        private readonly IJournal<T> _journal;
        private readonly JournalQueryable<T> _queryable;
        private readonly JournalQueryable<T> _queryableLatest;

        public Query(IJournal<T> journal, IReadTransactionContext transactionContext)
        {
            _transactionContext = transactionContext;
            _journal = journal;
            _queryable = new JournalQueryable<T>(
                new JournalQueryProvider<T>(_journal, transactionContext));

            if (_journal.Metadata.KeySymbol != null)
            {
                _queryableLatest = new JournalQueryable<T>(
                    JournalQueryProvider<T>.LatestBy(_journal.Metadata.KeySymbol, _journal, transactionContext));
            }
        }

        public ResultSet<T> AllBySymbolValueOverInterval(string symbol, 
            string value, DateInterval interval)
        {
            var intervalFilter = new PartitionIntervalIterator();
            var symbolFilter = new SymbolFilter(symbol, value);
            var parititionsFiltered = intervalFilter.IteratePartitions(
                _journal.Partitions.Reverse(), interval, _transactionContext);
            var ids = symbolFilter.Filter(parititionsFiltered, _transactionContext);

            return new ResultSet<T>(_journal, _transactionContext.ReadCache, ids);
        }

        public IQueryable<T> Items
        {
            get { return _queryable; }
        }

        public IQueryable<T> LatestByID
        {
            get { return _queryableLatest; }
        }

        public ResultSet<T> AllByKeyOverInterval(string value, DateInterval interval)
        {
            return AllBySymbolValueOverInterval(_journal.Metadata.KeySymbol, value, interval);
        }

        public ResultSet<T> AllBySymbol(string symbol, string value)
        {
            return AllBySymbolValueOverInterval(symbol, value, DateInterval.Any);
        }

        public ResultSet<T> All()
        {
            var paritionsAndMaxVals =
                _journal.Partitions.Select(p => Tuple.Create(p.PartitionID, 
                    _transactionContext.GetRowCount(p.PartitionID))).ToArray();

            long count = paritionsAndMaxVals.Sum(i => i.Item2);
            var ids = paritionsAndMaxVals.SelectMany(i => CreateRange(i.Item1, i.Item2));
            return new ResultSet<T>(_journal, _transactionContext.ReadCache, ids, count);
        }

        private IEnumerable<long> CreateRange(int partitionIndex, long itemsCount)
        {
            for (long i = 0; i < itemsCount; i++)
            {
                yield return RowIDUtil.ToRowID(partitionIndex, i);
            }
        }

        internal void RefreshContext(ITransactionContext transactionContext)
        {
            _transactionContext = transactionContext;
        }
    }
}