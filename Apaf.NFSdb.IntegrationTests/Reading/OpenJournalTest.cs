using System.Linq;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using NUnit.Framework;

namespace Apaf.NFSdb.IntegrationTests.Reading
{
    [TestFixture]
    public class OpenJournalTest
    {
        [Test]
        public void Open()
        {
            var j = new JournalBuilder()
                .WithLocation(@"C:\temp\QueryMonitor\VdbAlloc")
                .WithAccess(EFileAccess.Read)
                .ToJournal();

            using (var r = j.OpenRecordReadTx())
            {
                var records = r.Execute("Select status from VdbAlloc latest by correlationId");
                var allStatus = records.RecordIDs().Select(rid => records.Get<string>(rid, 0)).ToArray();
                int i = 0;
            }
        }

        [Test]
        public void OpenWrite()
        {
            var j = new JournalBuilder()
                .WithLocation(@"C:\temp\QueryMonitor\VdbAlloc")
                .ToJournal();

            using (var r = j.OpenRecordReadTx())
            {
                var records = r.Execute("Select status from VdbAlloc latest by correlationId");
                var allStatus = records.RecordIDs().Select(rid => records.Get<string>(rid, 0)).ToArray();
                int i = 0;
            }
        }
    }
}