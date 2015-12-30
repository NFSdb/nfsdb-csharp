using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Storage
{
    public static class StorageSizeUtils
    {
        public static int GetRecordSize(IColumnMetadata column, EDataType dataType)
        {
            if (column.DataType.ColumnType == EFieldType.BitSet)
                return column.AvgSize;

            if (column.DataType.IsFixedSize())
                return column.DataType.Size;

            if (dataType.IsFixedSize())
                return dataType.GetSize();

            if (column.DataType.ColumnType == EFieldType.Symbol && dataType == EDataType.Data)
                return EFieldType.Int32.GetSize();

            return -1;
        } 
    }
}