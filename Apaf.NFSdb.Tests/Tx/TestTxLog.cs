using System.Linq;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Tests.Tx
{
    public class TestTxLog
    {
        public static TransactionContext TestContext()
        {
            const int partitions = 100;
            const int files = 100;
            var paritionTx = Enumerable.Range(0, partitions)
                .Select(p => new PartitionTxData()
                {
                    AppendOffset = new long[files],
                    SymbolData = Enumerable.Range(0, files)
                        .Select(f => new SymbolTxData()).ToArray()
                }).ToArray();

            var tx = new TransactionContext(100, paritionTx);
            return tx;
        } 
    }
}