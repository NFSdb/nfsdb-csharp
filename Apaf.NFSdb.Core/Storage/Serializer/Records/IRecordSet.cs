using System.Collections.Generic;

namespace Apaf.NFSdb.Core.Storage.Serializer.Records
{
    public interface IRecordSet
    {
        IRecordSet Map(IList<string> columnNames);
        T Get<T>(long rowId, int columnIndex);
        IEnumerable<long> RecordIDs();
    }
}