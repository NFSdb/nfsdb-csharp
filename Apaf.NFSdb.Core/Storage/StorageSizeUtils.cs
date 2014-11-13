using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Storage
{
    public static class StorageSizeUtils
    {
        public static int GetRecordSize(ColumnMetadata column, EDataType dataType)
        {
            if (column.FieldType == EFieldType.BitSet)
                return column.AvgSize;

            if (column.FieldType.IsFixedSize())
                return column.FieldType.GetSize();

            if (dataType.IsFixedSize())
                return dataType.GetSize();

            if (column.FieldType == EFieldType.Symbol
                && dataType == EDataType.Data)
                return EFieldType.Int32.GetSize();

            return -1;
        } 
    }
}