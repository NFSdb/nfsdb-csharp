namespace Apaf.NFSdb.Core.Queries.Queryable.PlanItem
{
    public interface IColumnScanPlanItemCore
    {
        IPlanItem ToLastestByIdPlanItem();
        string SymbolName { get; }
    }
}