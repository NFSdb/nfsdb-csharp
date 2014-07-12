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
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.IntegrationTests
{
    public static class IntegrationTestConstants
    {
        public const int NULL_RECORD_COUNT = 3000;
        public static readonly long NULL_RECORD_FIRST_TIMESTAMP = 
            DateUtils.DateTimeToUnixTimeStamp(new DateTime(2013, 10, 5, 10, 0, 0));

        public static readonly long NULL_RECORD_TIMESTAMP_INCREMENT = 1000000;

        public static readonly string NULL_RECORD_FOLDER_NAME = "GenerateRecordsWithNulls";
        public static readonly string MULTI_PARITIONS_FOLDER_NAME = "GenerateMultiplePartitions";

        public static readonly string[] TEST_SYMBOL_LIST = { "AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L" };
    }
}