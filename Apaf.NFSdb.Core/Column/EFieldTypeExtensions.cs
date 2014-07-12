using System;

namespace Apaf.NFSdb.Core.Column
{
    public static class EFieldTypeExtensions
    {
        public static int GetSize(this EFieldType fieldType)
        {
            switch (fieldType)
            {
                case EFieldType.Byte:
                    return 1;
                case EFieldType.Bool:
                    return 1;
                case EFieldType.Int32:
                    return 4;
                case EFieldType.Int64:
                    return 8;
                case EFieldType.Int16:
                    return 2;
                case EFieldType.Double:
                    return 8;
                default:
                    throw new ArgumentOutOfRangeException("fieldType");
            }
        }
        public static bool IsFixedSize(this EFieldType fieldType)
        {
            switch (fieldType)
            {
                case EFieldType.Byte:
                case EFieldType.Bool:
                case EFieldType.Int32:
                case EFieldType.Int64:
                case EFieldType.Int16:
                case EFieldType.Double:
                    return true;
                default:
                    return false;
            }
        }
    }
}