#region copyright
/*
 * Copyright (c) 2014. APAF http://apafltd.co.uk
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
                case EFieldType.DateTime:
                case EFieldType.DateTimeEpochMs:
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
                case EFieldType.DateTime:
                    return true;
                default:
                    return false;
            }
        }
    }
}