using System;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Collections;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Collections
{
    [TestFixture]
    public class PriorityQueueTests
    {
        [TestCase(10, 10)]
        [TestCase(50, 10)]
        [TestCase(9, 10)]
        [TestCase(8, 10)]
        [TestCase(7, 10)]
        public void ShouldSortAsc(int len, int max)
        {
            var r = new Random(1);
            var testArray = Enumerable.Range(0, len).Select(i => r.Next(max)).ToArray();
            var pq = CreatePQ();

            for (int i = 0; i < testArray.Length; i++)
            {
                pq.Enqueue(testArray[i]);
            }

            var result = new int[testArray.Length];
            for (int i = 0; i < testArray.Length; i++)
            {
                result[i] = pq.Dequeue();
            }
            Array.Sort(testArray);
            // Array.Reverse(testArray);
            Assert.That(string.Join(",", result), Is.EqualTo(string.Join(",", testArray)));
        }

        [TestCase(10, 10)]
        [TestCase(50, 10)]
        [TestCase(9, 10)]
        public void ShouldSortDesc(int len, int max)
        {
            var r = new Random(1);
            var testArray = Enumerable.Range(0, len).Select(i => r.Next(max)).ToArray();
            var pq = CreatePQDesc();

            for (int i = 0; i < testArray.Length; i++)
            {
                pq.Enqueue(testArray[i]);
            }

            var result = new int[testArray.Length];
            for (int i = 0; i < testArray.Length; i++)
            {
                result[i] = pq.Dequeue();
            }
            Array.Sort(testArray);
            Array.Reverse(testArray);
            Assert.That(string.Join(",", result), Is.EqualTo(string.Join(",", testArray)));
        }

        private PriorityQueue<int> CreatePQDesc()
        {
            return new PriorityQueue<int>(new DescComparer());

        }

        private PriorityQueue<int> CreatePQ()
        {
            return new PriorityQueue<int>();
        }
    }

    internal class DescComparer : IComparer<int>
    {
        public int Compare(int x, int y)
        {
            return y.CompareTo(x);
        }
    }
}