#region copyright
/*
 * Copyright (c) 2014. APAF (Alex Pelagenko).
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
using NUnit.Framework;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Tests.Columns
{
    [TestFixture]
    public class ByteArrayTests
    {
        [TestCase(64 + 0, ExpectedResult = false)]
        [TestCase(64 + 7, ExpectedResult = true)]
        [TestCase(64 + 8, ExpectedResult = true)]
        [TestCase(64 + 12, ExpectedResult = false)]
        [TestCase(64 + 31, ExpectedResult = true)]
        public bool IsSet_Should_Return_Valid_Values(int bit)
        {
            var bytes = new byte[]
            {
                // First 64 bits are emtpy
                0, 0, 0, 0, 0, 0, 0, 0,
                // Then 4 filled bits
                Convert.ToByte("11110000", 2),
                Convert.ToByte("00001111", 2),
                Convert.ToByte("01010101", 2),
                Convert.ToByte("11111111", 2),
                // Then 4 empty
                0, 0 , 0, 0
            };

            var byteAr = new ByteArray(bytes);
            return byteAr.IsSet(bit);
        }

        [TestCase(new[] {0}, ExpectedResult = "00000001|00000000")]
        [TestCase(new[] { 0, 15 }, ExpectedResult = "00000001|10000000")]
        [TestCase(new[] { 0, 5, 15 }, ExpectedResult = "00100001|10000000")]
        [TestCase(new[] { 0, 5, 8, 15 }, ExpectedResult = "00100001|10000001")]
        public string Should_Set_Correct_Bit_To_True(int[] bits)
        {
            var bytes = new byte[2];

            var byteAr = new ByteArray(bytes);
            foreach (var bit in bits)
            {
                byteAr.Set(bit, true);
            }
            return Convert.ToString(bytes[0], 2).PadLeft(8, '0') 
                + "|" + Convert.ToString(bytes[1], 2).PadLeft(8, '0');
        }



        [TestCase(new[] { 0 }, ExpectedResult = "11111110|11111111")]
        [TestCase(new[] { 0, 15 }, ExpectedResult = "11111110|01111111")]
        [TestCase(new[] { 0, 5, 15 }, ExpectedResult = "11011110|01111111")]
        [TestCase(new[] { 0, 5, 8, 15 }, ExpectedResult = "11011110|01111110")]
        public string Should_Set_Correct_Bit_To_False(int[] bits)
        {
            var bytes = new byte[2] { 255, 255};

            var byteAr = new ByteArray(bytes);
            foreach (var bit in bits)
            {
                byteAr.Set(bit, false);
            }
            return Convert.ToString(bytes[0], 2).PadLeft(8, '0')
                + "|" + Convert.ToString(bytes[1], 2).PadLeft(8, '0');
        }
    }
}