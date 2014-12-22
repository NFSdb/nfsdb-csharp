using System.Collections.Generic;

namespace Apaf.NFSdb.Tests.Common
{
    public static class TestUtils
    {
        public static IEnumerable<KeyValuePair<string, long>> SplitNameSize(string fileNameSize)
        {
            var fileNameParts = fileNameSize.Split('|');
            var sizes = new List<KeyValuePair<string, long>>();
            foreach (var fileSize in fileNameParts)
            {
                var fileSizeArr = fileSize.Trim().Split('-');
                if (fileSizeArr.Length > 1)
                {
                    sizes.Add(
                        new KeyValuePair<string, long>(fileSizeArr[0].Trim(), 
                            long.Parse(fileSizeArr[1].Trim())));
                }
            }
            return sizes;
        }
    }
}