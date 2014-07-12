using System;

namespace Apaf.NFSdb.Core.Storage
{
    public static class EDataTypeExtensions
    {
        public static bool IsFixedSize(this EDataType dataType)
        {
            switch (dataType)
            {
                case EDataType.Symd:
                case EDataType.Symrk:
                case EDataType.Symrr:
                case EDataType.Symi:
                case EDataType.Datar:
                case EDataType.Datak:
                case EDataType.Data:
                    return false;

                case EDataType.Index:
                    return true;

                default:
                    throw new ArgumentOutOfRangeException("dataType");
            }
        }

        public static int GetSize(this EDataType dataType)
        {
            switch (dataType)
            {
                case EDataType.Index:
                    return 8;

                default:
                    throw new ArgumentOutOfRangeException("dataType");
            }
        }
    }
}