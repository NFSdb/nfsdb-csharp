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
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Tests.Storage;
using Apaf.NFSdb.Tests.Tx;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Columns
{
    [TestFixture]
    public class StringColumnTests
    {
        private IReadContext _readerContext;

        [TestCase("bla bla", 122)]
        [TestCase("bla bla", 256)]
        [TestCase("bla bla", ushort.MaxValue + 1)]
        [TestCase("Шукайте в Google швидше.", 500)]
        [TestCase("", 255)]
        [TestCase("", 0)]
        [TestCase("", 456)]
        [TestCase("", (int)1E6)]
        [TestCase("", 255)]
        [TestCase("", 144)]
        [TestCase("", 1024)]
        [TestCase("", (int)1E6)]
        public void ShouldReadString(string value, int maxSize)
        {
            var stringCol = CreateStringColumn(value, maxSize);
            var value2 = stringCol.GetValue(0, _readerContext);
            Assert.That(value2, Is.EqualTo(value));
        }

        [TestCase("a", 28)]
        [TestCase("ab", 28)]
        [TestCase("ab asdj lisdn. Шукайте в Google швидше. Встановіть пошук Google за замовчанням.", 500)]
        [TestCase("Шукайте в Google швидше.", 500)]
        public void ShouldWriteToString(string value, int maxLen)
        {
            var stringCol = CreateStringColumn(maxLen);
            var readContext = TestTxLog.TestContext();
            readContext.SetCurrentPartition(1);
            stringCol.SetValue(0, value, readContext);

            Assert.That(stringCol.GetValue(0, readContext.ReadCache), Is.EqualTo(value));
        }

        [Test]
        public unsafe void ByteShift()
        {
            string value = "Шукайте в Google швидше.";
            byte[] charBytes = new byte[value.Length*2];
            byte[] charBytes2 = new byte[value.Length * 2];
            fixed (char* charPtr = value)
            {
                byte* charBPtr = (byte*)charPtr;
                fixed (byte* bytePtr = charBytes)
                {
                    for (int i = 0; i < charBytes.Length; i+=2)
                    {
                        bytePtr[i] = charBPtr[i+1];
                        bytePtr[i+1] = charBPtr[i];
                    }
                }
                Buffer.BlockCopy(charBytes, 0, charBytes2, 0, charBytes.Length);

                
            }

            fixed (byte* bytePtr2 = charBytes2)
            {
                for (int i = 0; i < charBytes2.Length; i+=2)
                {
                    byte t = bytePtr2[i];
                    bytePtr2[i] = bytePtr2[i + 1];
                    bytePtr2[i + 1] = t;
                }
                string copyStr = new string((char*)(bytePtr2));
                Assert.That(copyStr, Is.EqualTo(value));
            }
        }
        
        private StringColumn CreateStringColumn(int maxLen)
        {
            _readerContext = new ReadContext();
            var data = new BufferBinaryReader(new byte[maxLen * 2 + 5]);
            var index = new BufferBinaryReader(new byte[2048]);

            return new StringColumn(data, index, maxLen, "column1");
        }

        private unsafe StringColumn CreateStringColumn(string value, int maxLen)
        {
            var data = new BufferBinaryReader(new byte[maxLen * 2 + 5]);
            var index = new BufferBinaryReader(new byte[2048]);

            _readerContext = new ReadContext();

            int headLength = CreateHeader(value, data);
            if (value != null)
            {
                fixed (char* chars = value)
                {
                    var strBytes = (byte*)&chars[0];
                    data.WriteBytes(headLength, strBytes, 0, value.Length * 2);
                }
            }

            return new StringColumn(data, index, maxLen, "column1");
        }

        [Test]
        public unsafe void MutatingString()
        {
            var str = new string(new []{'o', 'n', 'e'});
            fixed (char* ptr = str)
            {
                char t = ptr[0];
                ptr[0] = ptr[1];
                ptr[1] = t;
            }
            Console.WriteLine(str);
            Assert.That(str, Is.EqualTo("noe"));
        }

        private int CreateHeader(string value, IRawFile data)
        {
            if (value == null)
            {
                return StringColumn.HEADER_SIZE;
            }

            int length = value.Length;
            data.WriteInt32(0, length);
            return StringColumn.HEADER_SIZE;
        }
    }
}