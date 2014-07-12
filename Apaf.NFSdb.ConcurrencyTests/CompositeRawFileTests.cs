using System.Linq;
using System.Threading.Tasks;
using Apaf.NFSdb.Core.Storage;
using Microsoft.Concurrency.TestTools.UnitTesting.Chess;
using NUnit.Framework;

namespace Apaf.NFSdb.ConcurrencyTests
{
    [TestFixture]
    public class CompositeRawFileTests
    {
        [ChessTestMethod]
        [Test]
        public void ConcurrencyReads()
        {
            const int bitHint = 10;
            using (var compositeFile = new CompositeRawFile(
                "compTestFile1.d", bitHint, new CompositeFileFactory(),
                EFileAccess.Read, 1, 1, 1, EDataType.Data
                ))
            {
                var readOffsets = Enumerable.Range(0, CompositeRawFile.INITIAL_PARTS_COLLECTION_SIZE + 5)
                    .Select(i => (long) ((1 >> bitHint)*i)).ToArray();

                var tsk1 = Task.Factory.StartNew(() =>
                {
                    return readOffsets.Select(compositeFile.ReadInt64).ToArray();
                });

                readOffsets.Select(compositeFile.ReadInt64).ToArray();
                tsk1.Wait();
            }
        }

        [ChessTestMethod]
        [Test]
        public void FlushWithReads()
        {
            const int bitHint = 10;
            using (var compositeFile = new CompositeRawFile(
                "compTestFile1.d", bitHint, new CompositeFileFactory(),
                EFileAccess.ReadWrite, 1, 1, 1, EDataType.Data
                ))
            {
                var readOffsets = Enumerable.Range(0, 30)
                    .Select(i => (long) ((1 >> bitHint)*i)).ToArray();

                foreach (var readOffset in readOffsets)
                {
                    compositeFile.WriteInt64(readOffset, readOffset);
                }

                var tsk1 = Task.Factory.StartNew(() =>
                {
                    readOffsets.Select(i =>
                    {
                        var res = compositeFile.ReadInt64(i);
                        Assert.AreEqual(i, res);
                        return res;
                    }).ToArray();
                });

                for (int i = 0; i < 5; i++)
                {
                    compositeFile.Flush();
                }
                tsk1.Wait();
            }
        }
    }
}