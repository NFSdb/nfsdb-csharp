using System;
using System.Linq;
using Apaf.NFSdb.Core.Collections;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Collections
{
    [TestFixture]
    public class ArraySliceTests
    {
        [TestCase(0, 100, true, Result = 100)]
        [TestCase(10, 90, true, Result = 90)]
        [TestCase(10, 70, true, Result = 70)]
        [TestCase(0, 80, true, Result = 80)]
        [TestCase(10, 80, false, Result = 80)]
        public int ShouldReturnCorrectCount(int start, int len, bool asc)
        {
            var array = new int[100];
            return new ArraySlice<int>(array, start, len, asc).Count;
        }

        [TestCase(0, 100, true, 0, Result = 0)]
        [TestCase(0, 100, true, 98, Result = 98)]
        [TestCase(10, 90, true, 15, Result = 25)]
        [TestCase(10, 90, true, 90, ExpectedException = typeof(IndexOutOfRangeException))]
        [TestCase(10, 10, true, 11, ExpectedException = typeof(IndexOutOfRangeException))]
        [TestCase(10, 90, false, 9, Result = 90)]
        [TestCase(10, 10, false, 9, Result = 10)]
        [TestCase(10, 10, false, 10, ExpectedException = typeof(IndexOutOfRangeException))]
        public int ShouldReturnCorrectIndex(int start, int len, bool asc, int index)
        {
            var array = Enumerable.Range(0, 100).ToArray();

            return new ArraySlice<int>(array, start, len, asc)[index];
        }


        [TestCase(0, 2, true, 2, Result = "0,1")]
        [TestCase(0, 2, false, 2, Result = "1,0")]
        [TestCase(1, 3, false, 3, Result = "3,2,1")]
        public string ShouldCopyTo(int start, int len, bool asc, int len2)
        {
            var array = Enumerable.Range(0, 100).ToArray();

            var slice = new ArraySlice<int>(array, start, len, asc);
            var array2 = new int[len2];
            slice.CopyTo(array2, 0);

            return string.Join(",", array2);
        }


        [TestCase(0, 2, true, Result = "0,1")]
        [TestCase(0, 2, false, Result = "1,0")]
        [TestCase(1, 3, false, Result = "3,2,1")]
        public string ShouldEnumerate(int start, int len, bool asc)
        {
            var array = Enumerable.Range(0, 100).ToArray();

            var slice = new ArraySlice<int>(array, start, len, asc);
            return string.Join(",", slice);
        }
    }
}