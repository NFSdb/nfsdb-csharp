namespace Apaf.NFSdb.Core.Column
{
    public static class MetadataConstants
    {
        public static readonly string FILE_EXTENSION_DATA = ".d";
        public static readonly string FILE_EXTENSION_INDEX = ".i";
        public static readonly string FILE_EXTENSION_SYMI = ".symi";
        public static readonly string FILE_EXTENSION_SYMD = ".symd";
        public static readonly string FILE_EXTENSION_SYMRK = ".symr.k";
        public static readonly string FILE_EXTENSION_SYMRR = ".symr.r";
        public static readonly string FILE_EXTENSION_DATAK = ".k";
        public static readonly string FILE_EXTENSION_DATAR = ".r";

        public static readonly string NULLS_FILE_NAME = "_nulls";
        public static readonly string DEFAULT_PARTITION_DIR = "default";
        public static readonly int ISSET_HEADER_LENGTH = 0; //4;
        public static readonly string ISSET_FIELD_NAME = "__isset";

        public static readonly int DEFAULT_AVG_RECORD_SIZE = 0xff;
        public static readonly int DEFAULT_DISTINCT_HINT_COUNT = 255;
        public static readonly int DEFAULT_OPEN_PARTITION_TTL = 60;
        public static readonly int DEFAULT_RECORD_HINT = (int) 1E6;
        public static readonly int DEFAULT_SYMBOL_MAX_SIZE = 128;
        public static readonly int DEFAULT_STRING_AVG_SIZE = 12;
        public static readonly int DEFAULT_STRING_MAX_SIZE = 255;
        public static readonly int DEFAULT_SYMBOL_AVG_SIZE = DEFAULT_STRING_AVG_SIZE;
        public static readonly int DEFAULT_MAX_OPEN_PARTITIONS = -1;
        public static readonly int DEFAULT_LAG_HOURS = 0;

        public static readonly int STRING_AVG_HEADER = 2;
        public const int STRING_INDEX_FILE_RECORD_SIZE = 8;
        public const int STRING_BYTE_LIMIT = byte.MaxValue;
        public const int STRING_TWO_BYTE_LIMIT = ushort.MaxValue;
        public const int STRING_NULL_VALUE = -1;
    
        public static readonly int SYMBOL_STRING_CACHE_SIZE = 500;
        public static readonly string TEMP_DIRECTORY_PREFIX = "temp";
        public static readonly int HASH_FUNCTION_GROUPING_RATE = 25;
        public const int STRING_HASH_CODE_SOLT = 0x7fffffff;

        public const int SYMBOL_PARTITION_ID = 0;

        public const long FILE_HEADER_LENGTH = 8;
        public const string PARTITION_TYPE_FILENAME = "_partition_type";

        public static readonly int PIPE_BIT_HINT = 16;
        public static readonly int TX_LOG_FILE_ID = -1;
        public static readonly string TX_FILE_NAME = "_tx";
        public const int K_FILE_KEY_BLOCK_HEADER_SIZE = 16;
        public const int K_FILE_KEY_BLOCK_OFFSET = 8;
        public const int K_FILE_ROW_BLOCK_LEN_OFFSET = 0;
        public const int NULL_SYMBOL_VALUE = -1;
        public const int SYMBOL_NOT_FOUND_VALUE = -1;
    }
}