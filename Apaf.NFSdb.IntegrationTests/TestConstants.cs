using System;
using System.Diagnostics;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.IntegrationTests
{
    public class TestUtils
    {
        public const int GENERATE_RECORDS_COUNT = (int)1E6;
        public static readonly DateTime START = new DateTime(DateTime.Now.AddYears(-1).Year,
            DateTime.Now.AddYears(-1).Month, 1);
        public static readonly long START_TIMESTAMP = DateUtils.DateTimeToUnixTimeStamp(START);

        public static TimeSpan GenerateRecords<T>(IJournal<T> journal, 
            int count, int partitionCount, Action<T, long, int> generateValue) 
            where T : new()
        {
            var increment = GetTimestampIncrement(count, partitionCount);
            var stopwatch = new Stopwatch();
            using (var wr = journal.OpenWriteTx())
            {
                stopwatch.Start();
                var quote = new T();
                for (int i = 0; i < count; i++)
                {
                    generateValue(quote, increment, i);
                    wr.Append(quote);
                }
                wr.Commit();
                stopwatch.Stop();
            }
            return stopwatch.Elapsed;
        }



        public static long GetTimestampIncrement(long count, int partitionCount)
        {
            return (long)((START.AddMonths(partitionCount).AddDays(-1) - START).TotalMilliseconds / count);
        }

    }
}