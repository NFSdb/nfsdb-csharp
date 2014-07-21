#region copyright
/*
 * Copyright (c) 2014. APAF http://apafltd.co.uk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion
using System;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class SymbolFilter : IPartitionFilter
    {
        private const int LOOP_SORT_THRESHOLD = 5;
        private readonly string _symbol;
        private readonly string[] _values;

        public SymbolFilter(string symbol, string value)
        {
            _symbol = symbol;
            _values = new [] {value};
        }

        public SymbolFilter(string symbol, string[] values)
        {
            _symbol = symbol;
            _values = values;
        }

        public IEnumerable<long> Filter(IEnumerable<PartitionRowIDRange> partitions, 
            IReadTransactionContext tx)
        {
            var pp = partitions.SelectMany(part =>
                MergeSorted(
                    _values
                    .Select(v => part.Partition.GetSymbolRows(_symbol, v, tx)
                    .Where(rowid => rowid >= part.Low && rowid <= part.High)
                    .Select(row => RowIDUtil.ToRowID(part.Partition.PartitionID, row))).ToArray()));
            
            return pp;
        }

        private IEnumerable<long> MergeSorted(IEnumerable<long>[] items)
        {
            if (items.Length < LOOP_SORT_THRESHOLD)
            {
                IEnumerable<long> result = items[0];
                for (int i = 1; i < items.Length; i++)
                {
                    result = MergeDistinct(result, items[i]);
                }
                return result;
            }
            return GetMergeSorted(items);
        }

        private IEnumerable<long> GetMergeSorted(IEnumerable<long>[] items)
        {
            var nextVals = new long[items.Length];
            var hasFinished = new bool[items.Length];
            var ens = items.Select(i => i.GetEnumerator()).ToArray();
            while (true)
            {
                int j = 0;
                for (int i = 0; i < ens.Length; i++)
                {
                    if (!hasFinished[i] && ens[i].MoveNext())
                    {
                        nextVals[j++] = ens[i].Current;
                    }
                    else
                    {
                        hasFinished[i] = true;
                    }
                }
                if (j == 0)
                {
                    yield break;
                }
                if (j > 1)
                {
                    Array.Sort(nextVals, 0, j);
                }

                for (int i = j - 1; i >= 0; i--)
                {
                    yield return nextVals[i];
                }
            }
        }

        private IEnumerable<long> MergeDistinct(IEnumerable<long> enum1, IEnumerable<long> enum2)
        {
            var e1 = enum1.GetEnumerator();
            var e2 = enum2.GetEnumerator();

            var e1Next = e1.MoveNext();
            var e2Next = e2.MoveNext();

            var ei1 = e1Next ? e1.Current : long.MinValue;
            var ei2 = e2Next ? e2.Current : long.MinValue;

            while (e1Next || e2Next)
            {
                if (ei1 > ei2)
                {
                    yield return ei1;
                    e1Next = e1.MoveNext();
                    ei1 = e1Next ? e1.Current : long.MinValue;
                }
                else
                {
                    yield return ei2;
                    e2Next = e2.MoveNext();
                    ei2 = e2Next ? e2.Current : long.MinValue;
                }
            }
        }
    }
}