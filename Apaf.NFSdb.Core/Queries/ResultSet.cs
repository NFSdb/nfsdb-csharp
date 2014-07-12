using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Queries
{
    public class ResultSet<T> : IEnumerable<T>
    {
        public enum Order
        {
            Asc,
            Desc
        }

        private readonly IJournal<T> _journal;
        private readonly IReadContext _readContext;
        private readonly IEnumerable<long> _rowIDs;

        public ResultSet(IJournal<T> journal, IReadContext readContext, IEnumerable<long> rowIDs)
        {
            _journal = journal;
            _readContext = readContext;
            _rowIDs = rowIDs;
        }

        public ResultSet(IJournal<T> journal, IReadContext readContext, IEnumerable<long> rowIDs, long length)
        {
            _journal = journal;
            _readContext = readContext;
            _rowIDs = rowIDs;
            Length = length;
        }

        public ResultSet(Journal<T> journal, IReadContext readContext)
        {
            _journal = journal;
            _readContext = readContext;
            _rowIDs = new long[] {};
        }

        public long? Length { get; protected set; }

        public IJournal<T> Journal
        {
            get { return _journal; }
        }

        private IEnumerable<T> Read()
        {
            return _journal.Read(_rowIDs, _readContext);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return Read().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public RandomAccessResultSet<T> ToRandomAccess()
        {
            return new RandomAccessResultSet<T>(_journal, _rowIDs.ToArray(), _readContext);
        }
    }
}
