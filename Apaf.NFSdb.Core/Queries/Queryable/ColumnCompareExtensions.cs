using System;
using System.Collections.Generic;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public static class ColumnCompareExtensions
    {
        public static IComparer<long> GetComparer(this ColumnMetadata column, IReadTransactionContext tx, bool ascending)
        {
            switch (column.FieldType)
            {
                case EFieldType.Byte:
                    return new ColumnValueComparer<byte>(column.FieldID, tx, ascending);
                case EFieldType.Bool:
                    return new ColumnValueComparer<bool>(column.FieldID, tx, ascending);
                case EFieldType.Int16:
                    return new ColumnValueComparer<short>(column.FieldID, tx, ascending);
                case EFieldType.Int32:
                    return new ColumnValueComparer<int>(column.FieldID, tx, ascending);
                case EFieldType.Int64:
                    return new ColumnValueComparer<long>(column.FieldID, tx, ascending);
                case EFieldType.Double:
                    return new ColumnValueComparer<double>(column.FieldID, tx, ascending);
                case EFieldType.Symbol:
                case EFieldType.String:
                    return new ColumnValueComparer<string>(column.FieldID, tx, ascending);
                case EFieldType.DateTime:
                case EFieldType.DateTimeEpochMilliseconds:
                    return new ColumnValueComparer<DateTime>(column.FieldID, tx, ascending);
                case EFieldType.Binary:
                    return new BinaryColumnValueComparer(column.FieldID, tx, ascending);

                default:
                    throw new NFSdbQueryableNotSupportedException("Column {0} of type {1} is not sortable.", column.PropertyName, column.FieldType);
            }
        }
    }
}