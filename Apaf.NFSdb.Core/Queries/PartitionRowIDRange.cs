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

using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Queries
{
    public class PartitionRowIDRange
    {
        public PartitionRowIDRange(IPartitionCore part)
        {
            Partition = part;
            Low = 0;
            High = long.MaxValue;
        }

        public PartitionRowIDRange(IPartitionCore part, long low, long high)
        {
            Partition = part;
            Low = low;
            High = high;
        }

        public IPartitionCore Partition { get; private set; }
        public long Low { get; private set; }
        public long High { get; private set; }
    }
}