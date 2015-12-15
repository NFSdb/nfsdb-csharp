using System;
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Storage.Serializer.Records;

namespace Apaf.NFSdb.Core.Queries
{
    public interface IRecordQuery : IDisposable
    {
        IRecordSet Execute(string query);
        IRecordSet Execute(string query, QlParameter[] parameters);
    }
}