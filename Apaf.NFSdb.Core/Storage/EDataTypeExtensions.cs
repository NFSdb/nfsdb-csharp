#region copyright
/*
 * Copyright (c) 2014. APAF (Alex Pelagenko).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion
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