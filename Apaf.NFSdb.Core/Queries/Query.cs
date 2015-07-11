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
using System;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class Query<T> : IQuery<T>
    {
        private IReadTransactionContext _transactionContext;
        private readonly IJournal<T> _journal;
        private readonly Lazy<JournalQueryable<T>> _queryable;
        private readonly Dictionary<string, JournalQueryable<T>> _latestBySymbol = new Dictionary<string, JournalQueryable<T>>();

        public Query(IJournal<T> journal, IReadTransactionContext transactionContext)
        {
            _transactionContext = transactionContext;
            _journal = journal;
            _queryable = new Lazy<JournalQueryable<T>>(
                () => new JournalQueryable<T>(new JournalQueryProvider<T>(_journal, transactionContext)));
            _transactionContext.AddRefsAllPartitions();
        }

        public ResultSet<T> AllBySymbolValueOverInterval(string symbol, string value, DateInterval interval)
        {
            _transactionContext.AddRefsAllPartitions();

            var intervalFilter = new PartitionIntervalIterator();
            var symbolFilter = new SymbolFilter(symbol, value);
            var parititionsFiltered = intervalFilter.IteratePartitions(
                _transactionContext.ReverseReadPartitions, interval, _transactionContext);
            var ids = symbolFilter.Filter(parititionsFiltered, _transactionContext);

            return ResultSetFactory.Create<T>(ids, _transactionContext);
        }

        public int PartitionCount
        {
            get { return _transactionContext.PartitionCount; }
        }

        public IQueryable<T> Items
        {
            get { return _queryable.Value; }
        }

        public IQueryable<T> GetLatestItemsBy(string columnName)
        {
            var column = _journal.Metadata.Columns.FirstOrDefault(c => c.PropertyName == columnName);
            if (column == null)
            {
                throw new NFSdbConfigurationException("Column {0} does not exists", columnName);
            }

            if (!column.Indexed)
            {
                throw new NFSdbConfigurationException("Column {0} is not indexed", columnName);
            }

            JournalQueryable<T> val;
            if (!_latestBySymbol.TryGetValue(columnName, out val))
            {
                val = _latestBySymbol[columnName] = new JournalQueryable<T>(
                    JournalQueryProvider<T>.LatestBy(columnName, _journal, _transactionContext));
            }
            return val;
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
                _transactionContext.ReadPartitions.Select(p => Tuple.Create(p, 
                    _transactionContext.GetRowCount(p.PartitionID))).ToArray();

            long count = paritionsAndMaxVals.Sum(i => i.Item2);
            var ids = paritionsAndMaxVals.SelectMany(i => CreateRange(i.Item1.PartitionID, i.Item2));
            return ResultSetFactory.Create<T>(ids, _transactionContext, count);
        }

        public ResultSet<T> Enumerate()
        {
            var paritionsAndMaxVals =
                _transactionContext.ReadPartitions.Select(p => Tuple.Create(p,
                    _transactionContext.GetRowCount(p.PartitionID)));

            var ids = paritionsAndMaxVals.SelectMany(i => CreateRange(i.Item1.PartitionID, i.Item2));
            return ResultSetFactory.Create<T>(ids, _transactionContext);
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

        public void Dispose()
        {
            _transactionContext.Dispose();
        }
    }
}