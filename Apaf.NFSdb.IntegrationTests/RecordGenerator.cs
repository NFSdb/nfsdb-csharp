using System;
using System.Diagnostics;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.IntegrationTests
{
    public abstract class RecordGenerator<T> where T :new()
    {
        public TimeSpan GenerateRecords(IJournal<T> journal,
            int count, int partitionCount, int paritionTtl = MetadataConstants.DEFAULT_OPEN_PARTITION_TTL)
        {
            var increment = TestUtils.GetTimestampIncrement(count, partitionCount);
            var stopwatch = new Stopwatch();
            using (var wr = journal.OpenWriteTx(paritionTtl))
            {
                stopwatch.Start();
                var itme = new T();
                for (int i = 0; i < count; i++)
                {
                    var timestamp = DateUtils.UnixTimestampToDateTime(
                        TestUtils.START_TIMESTAMP + increment*i);
                    
                    GenerateValues(itme, timestamp, i);
                    wr.Append(itme);
                }
                wr.Commit();
                stopwatch.Stop();
            }
            return stopwatch.Elapsed;
        }

        protected abstract void GenerateValues(T item, DateTime timestamp, int i);
    }
}