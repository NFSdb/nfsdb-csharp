using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Queries
{
    public interface ILatestBySymbolFilter
    {
        ColumnMetadata Column { get; }
    }
}