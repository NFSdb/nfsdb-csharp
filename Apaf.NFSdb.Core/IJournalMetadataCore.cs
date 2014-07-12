using System.Collections.Generic;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;

namespace Apaf.NFSdb.Core
{
    public interface IJournalMetadataCore
    {
        IEnumerable<ColumnMetadata> Columns { get; }
        JournalSettings Settings { get; }
        int? TimestampFieldID { get; }
        ColumnMetadata GetColumnById(int columndID);
        int GetFieldID(string arg);
        string KeySymbol { get; }
        int FileCount { get; }
    }
}