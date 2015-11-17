using System.Collections.Generic;

namespace Apaf.NFSdb.Core.Queries.Records
{
    public interface IRecordSet
    {
        void Map(IList<string> columnNames);
        T Get<T>(long rowId, int columnIndex);
        IEnumerable<long> RecordIDs();
    }
}