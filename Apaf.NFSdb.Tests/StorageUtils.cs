using System.Collections.Generic;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Tests
{
    public static class StorageUtils
    {
        public static IEnumerable<IRawFile> AllOpenedFiles(IColumnStorage cs)
        {
            for (int i = 0; i < cs.OpenFileCount; i++)
            {
                var file = cs.GetOpenedFileByID(i);
                if (file != null)
                {
                    yield return file;
                }
            }
        }
    }
}