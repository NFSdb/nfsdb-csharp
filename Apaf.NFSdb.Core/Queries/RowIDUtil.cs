﻿#region copyright
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
namespace Apaf.NFSdb.Core.Queries
{
    public static class RowIDUtil
    {
        public static int ToPartitionIndex(long rowID)
        {
            return (int)(rowID >> 44);
        }

        public static long ToLocalRowID(long rowID)
        {
            return rowID & 0xFFFFFFFFFFFL;
        }

        public static long ToRowID(int partitionIndex, long localRowID)
        {
            return (((long)partitionIndex) << 44) + localRowID;
        }

        public static long ToExternalRowID(int partitionIndex, long localRowID)
        {
            return (((long)partitionIndex - 1) << 44) + localRowID;
        }

        public static int ToPartitionIDFromExternalRowID(long externalRowID)
        {
            return (int)((externalRowID - 1) >> 44) + 1;
        }
    }
}