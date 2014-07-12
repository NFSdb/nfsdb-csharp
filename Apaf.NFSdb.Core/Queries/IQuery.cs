using System.Linq;

namespace Apaf.NFSdb.Core.Queries
{
    public interface IQuery<T>
    {
        ResultSet<T> All();
        ResultSet<T> AllBySymbol(string symbol, string value);
        ResultSet<T> AllByKeyOverInterval(string value, DateInterval interval);
        ResultSet<T> AllBySymbolValueOverInterval(string symbol, string values, DateInterval interval);

        IQueryable<T> Items { get; }
        IQueryable<T> LatestByID { get; }
    }
}