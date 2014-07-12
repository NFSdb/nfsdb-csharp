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