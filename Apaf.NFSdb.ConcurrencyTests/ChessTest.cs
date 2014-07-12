using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Concurrency.TestTools.UnitTesting.Chess;

namespace Apaf.NFSdb.ConcurrencyTests
{
    public class ChessTest
    {
        private volatile MockFile[] _files;

        [ChessTestMethod]
        [ExpectedChessResult("csb1", ChessExitCode.Success)]
        public void ConcurrencyTest()
        {
            const int length = 10;
            _files = Enumerable.Repeat(1, length).Select(i => new MockFile()).ToArray();

            var tsk2 = Task.Factory.StartNew(() =>
            {
                var oldFiles = _files;
                _files = Enumerable.Repeat(1, length).Select(i => new MockFile()).ToArray();
                foreach (var mockFile in oldFiles)
                {
                    mockFile.Dispose();
                }
            });

            for (int i = 0; i < _files.Length; i++)
            {
                try
                {
                    _files[i].Read();
                }
                catch (NullReferenceException)
                {
                    _files[i].Read();
                }
            }
            Task.WaitAll(tsk2);
        }

        public class MockFile : IDisposable
        {
            private volatile object value = 10;

            public void Dispose()
            {
                value = null;
            }

            public int Read()
            {
                return (int)value;
            }
        }
    }
}
