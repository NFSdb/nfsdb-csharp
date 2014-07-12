using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core
{
    public interface IQueryStatistics
    {
        long RowsBySymbolValue(IReadTransactionContext tx, string symbolName, string[] value);
        int GetSymbolCount(IReadTransactionContext tx, string symbolName);
    }
}