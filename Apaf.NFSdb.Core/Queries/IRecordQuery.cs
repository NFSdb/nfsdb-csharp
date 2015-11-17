using System;
using Apaf.NFSdb.Core.Queries.Records;

namespace Apaf.NFSdb.Core.Queries
{
    public interface IRecordQuery : IDisposable
    {
        IRecordSet Execute(string query);
    }
}