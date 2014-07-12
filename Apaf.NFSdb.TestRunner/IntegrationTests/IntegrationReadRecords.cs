using Apaf.NFSdb.IntegrationTests.JavaIntegration;

namespace Apaf.NFSdb.TestRunner.IntegrationTests
{
    public class IntegrationReadRecords : ITask
    {
        public string Name
        {
            get { return "integration-read-null-records"; }
        }

        public void Run()
        {
            var testFixture = new JavaReadTests();
            testFixture.Records_with_nulls();
            testFixture.Records_with_nulls_indexes();
        }
    }
}