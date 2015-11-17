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
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Queries.Records;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class Query<T> : IQuery<T>
    {
        private IReadTransactionContext _transactionContext;
        private readonly IJournal<T> _journal;
        private readonly Lazy<JournalQueryable<T>> _queryable;
        private readonly IRecordQuery _recordQuery;

        public Query(IJournal<T> journal, IReadTransactionContext transactionContext)
        {
            _transactionContext = transactionContext;
            _journal = journal;
            _queryable = new Lazy<JournalQueryable<T>>(
                () => new JournalQueryable<T>(new JournalQueryProvider<T>(_journal.Core, transactionContext)));
            _transactionContext.AddRefsAllPartitions();
            _recordQuery = new RecordQuery(journal.Core, transactionContext);
        }

        public ResultSet<T> AllBySymbolValueOverInterval<TT>(string symbol, TT value, DateInterval interval)
        {
            var column = _journal.Metadata.GetColumnByPropertyName(symbol);
            _transactionContext.AddRefsAllPartitions();

            var intervalFilter = new PartitionIntervalIterator();
            var symbolFilter = new SymbolFilter<TT>(column, value);
            var parititionsFiltered = intervalFilter.IteratePartitions(
                _transactionContext.ReverseReadPartitions, interval, _transactionContext);
            var ids = symbolFilter.Filter(parititionsFiltered, _transactionContext, ERowIDSortDirection.Desc);

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

        public IRecordQuery RecordQuery
        {
            get { return _recordQuery; }
        }

        public ResultSet<T> AllByKeyOverInterval<TT>(TT value, DateInterval interval)
        {
            return AllBySymbolValueOverInterval(_journal.Metadata.KeySymbol, value, interval);
        }

        public ResultSet<T> AllBySymbol<TT>(string symbol, TT value)
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

        public void Dispose()
        {
            _transactionContext.Dispose();
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