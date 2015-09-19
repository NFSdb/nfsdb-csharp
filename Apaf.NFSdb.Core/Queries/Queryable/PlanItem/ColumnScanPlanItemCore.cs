using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Queries.Queryable.PlanItem
{
    public interface IColumnScanPlanItemCore
    {
        bool CanTranformLastestByIdPlanItem(ColumnMetadata column);
        void TranformLastestByIdPlanItem(ColumnMetadata column);
    }
}