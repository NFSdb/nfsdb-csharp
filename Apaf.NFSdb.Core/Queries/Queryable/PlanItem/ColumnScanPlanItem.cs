using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable.PlanItem
{
    public class ColumnScanPlanItem : IPlanItem
    {
        private readonly string _memberName;
        private readonly string[] _literal;
        private long? _cardin;

        public ColumnScanPlanItem(string memberName, string[] literals)
        {
            _memberName = memberName;
            _literal = literals;
            Timestamps = new DateRange();
        }

        public string SymbolName { get { return _memberName; }}

        public IEnumerable<long> Execute(IJournalCore journal, IReadTransactionContext tx)
        {
            var intervalFilter = new PartitionIntervalIterator();
            var symbolFilter = new SymbolFilter(_memberName, _literal);

            return Timestamps.AllIntervals.Reverse().SelectMany(interval =>
                symbolFilter.Filter(intervalFilter.IteratePartitions(
                    journal.PartitionsCore.Reverse(), interval, tx), tx)
                );
        }

        public string[] Literals {get { return _literal; }}

        public long Cardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            if (!_cardin.HasValue)
            {
                _cardin = journal.QueryStatistics.RowsBySymbolValue(tx, _memberName, _literal);
            }
            return _cardin.Value;
        }

        public void Intersect(IPlanItem restriction)
        {
            Timestamps.Intersect(restriction.Timestamps);
        }

        public DateRange Timestamps { get; private set; }
    }
}