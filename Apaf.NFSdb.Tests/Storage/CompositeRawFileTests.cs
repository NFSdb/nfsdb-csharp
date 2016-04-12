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
using System.ComponentModel;
using System.IO;
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Moq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Storage
{
    [TestFixture]
    public class CompositeRawFileTests
    {
        private Mock<ICompositeFileFactory> _copositeFileFactory;
        private Mock<ICompositeFile> _compositeFile;

        // Reading 0 chunk
        [TestCase(0x100, (long)1E9, 0, 0, 0x100)]
        [TestCase(0x400 * 0x400, (long)1E9, 0, 0, 0x400 * 0x400)]
        [TestCase(0x400 * 0x400, 0x400 * 20, 0, 0, 0x400 * 20)]
        // Reading 1 chunk
        [TestCase(0x400, 0x400 + 367, 0x400, 0x400, 367)]
        // Reading 2nd chunk
        [TestCase(0x400, (long)1E9, 0x801, 0x800, 0x400)]
        public void ShouldCalculateChunkLenghtCorrectly(int recordsSize, long fileLength, long readOffset, long chunkStart, long chunkSize)
        {
            var compFile = CreateCompositeRawFile(fileLength, recordsSize);
            try
            {
                compFile.ReadByte(readOffset);
            }
            catch (NullReferenceException)
            {
            }
            _compositeFile.Verify(f => f.CreateViewAccessor(chunkStart, chunkSize), Times.Once);
        }


        [TestCase(0x400, false)]
        [TestCase(0x4000, true)]
        [TestCase(0x10000, true)]
        public void ShouldReadParallel(int fileChunkSize, bool reverse)
        {
            try
            {
                ClearTempFile();
                const long seed = 9238492384L;
                const int itemSize = 8;
                int itemsCount = (fileChunkSize * CompositeRawFile.INITIAL_PARTS_COLLECTION_SIZE + 1) / itemSize;

                var indexes = Enumerable.Range(0, itemsCount);
                if (reverse) indexes = indexes.Reverse();

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    foreach (var i in indexes)
                    {
                        compFile.WriteInt64(i * itemSize, i * seed);
                    }
                }

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    foreach (var i in indexes)
                    {
                        Assert.That(compFile.ReadInt64(i * itemSize), Is.EqualTo(i * seed));
                    }
                }
            }
            finally
            {
                ClearTempFile();
            }
        }


        [TestCase(0x400, false)]
        [TestCase(0x4000, true)]
        [TestCase(0x10000, true)]
        public void ShouldReadWriteLongs(int fileChunkSize, bool reverse)
        {
            try
            {
                const long seed = 9238492384L;
                const int itemSize = 8;
                int itemsCount = (fileChunkSize * CompositeRawFile.INITIAL_PARTS_COLLECTION_SIZE + 1) / itemSize;

                var indexes = Enumerable.Range(0, itemsCount);
                if (reverse) indexes = indexes.Reverse();

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    foreach (var i in indexes)
                    {
                        compFile.WriteInt64(i * itemSize, i * seed);
                    }
                }

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    //var partitons = Partitioner.Create(0, itemsCount);
                    //Parallel.ForEach(partitons, p =>
                    //{
                    //    for (int i = p.Item1; i < p.Item2; i++)
                    //    {
                    //        if (compFile.ReadInt64(i*itemSize) != i*seed)
                    //        {
                    //            throw new IndexOutOfRangeException();
                    //        }
                    //    }
                    //});
                }
            }
            finally
            {
                ClearTempFile();
            }
        }

        [TestCase(0x400, false)]
        [TestCase(0x4000, true)]
        [TestCase(0x10000, true)]
        public void ShouldReadWriteInts(int fileChunkSize, bool reverse)
        {
            try
            {
                ClearTempFile();
                const int seed = 923849238;
                const int itemSize = 4;
                int itemsCount = (fileChunkSize * CompositeRawFile.INITIAL_PARTS_COLLECTION_SIZE + 1) / itemSize;

                var indexes = Enumerable.Range(0, itemsCount);
                if (reverse) indexes = indexes.Reverse();

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    foreach (var i in indexes)
                    {
                        compFile.WriteInt32(i * itemSize, i * seed);
                    }
                }

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    foreach (var i in indexes)
                    {
                        Assert.That(compFile.ReadInt32(i * itemSize), Is.EqualTo(i * seed));
                    }
                }
            }
            finally
            {
                ClearTempFile();
            }
        }

        [TestCase(0x400, false)]
        [TestCase(0x4000, true)]
        [TestCase(0x10000, true)]
        public void ShouldReadWriteDouble(int fileChunkSize, bool reverse)
        {
            try
            {
                ClearTempFile();
                const double seed = 92384.9238;
                const int itemSize = 8;
                int itemsCount = (fileChunkSize * CompositeRawFile.INITIAL_PARTS_COLLECTION_SIZE + 1) / itemSize;

                var indexes = Enumerable.Range(0, itemsCount);
                if (reverse) indexes = indexes.Reverse();

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    foreach (var i in indexes)
                    {
                        compFile.WriteDouble(i * itemSize, i * seed);
                    }
                }

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    foreach (var i in indexes)
                    {
                        Assert.That(compFile.ReadDouble(i * itemSize), Is.EqualTo(i * seed));
                    }
                }
            }
            finally
            {
                ClearTempFile();
            }
        }

        [TestCase(0x400, false)]
        [TestCase(0x4000, true)]
        [TestCase(0x10000, true)]
        public void ShouldReadWriteInt16(int fileChunkSize, bool reverse)
        {
            try
            {
                const short seed = 93;
                const int itemSize = 2;
                int itemsCount = (fileChunkSize * CompositeRawFile.INITIAL_PARTS_COLLECTION_SIZE + 1) / itemSize;

                var indexes = Enumerable.Range(0, itemsCount);
                if (reverse) indexes = indexes.Reverse();

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    foreach (var i in indexes)
                    {
                        compFile.WriteInt16(i * itemSize, (short)(i * seed));
                    }
                }

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    foreach (var i in indexes)
                    {
                        Assert.That(compFile.ReadInt16(i * itemSize), Is.EqualTo((short)(i * seed)));
                    }
                }
            }
            finally
            {
                ClearTempFile();
            }
        }

        [TestCase(0x400, false)]
        [TestCase(0x4000, true)]
        [TestCase(0x10000, true)]
        public void ShouldReadWriteUInt16(int fileChunkSize, bool reverse)
        {
            try
            {
                const short seed = 93;
                const int itemSize = 2;
                int itemsCount = (fileChunkSize * CompositeRawFile.INITIAL_PARTS_COLLECTION_SIZE + 1) / itemSize;

                var indexes = Enumerable.Range(0, itemsCount);
                if (reverse) indexes = indexes.Reverse();

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    foreach (var i in indexes)
                    {
                        compFile.WriteUInt16(i * itemSize, (ushort)(i * seed));
                    }
                }

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    foreach (var i in indexes)
                    {
                        Assert.That(compFile.ReadUInt16(i * itemSize), Is.EqualTo((ushort)(i * seed)));
                    }
                }
            }
            finally
            {
                ClearTempFile();
            }
        }

        [TestCase(10, 0, Result = 1016)]
        [TestCase(10, 1016, Result = 1024)]
        public long ShouldCalcBufferEnd(int bitHint, long offset)
        {
            return (1 << bitHint) - ((offset + MetadataConstants.FILE_HEADER_LENGTH) & ((1 << bitHint) - 1));
        }

        [TestCase(0x400, 11, false)]
        [TestCase(0x800, 195, true)]
        [TestCase(0x800, 0x801, true)]
        public void ShouldReadWriteByteArrays(int fileChunkSize, int itemSize, bool reverse)
        {
            try
            {
                var writeArray = Enumerable.Range(0, itemSize).Select(a => (byte)a).ToArray();
                const short seed = 93;
                int itemsCount = (fileChunkSize * CompositeRawFile.INITIAL_PARTS_COLLECTION_SIZE + 1) / itemSize;

                var indexes = Enumerable.Range(0, itemsCount);
                if (reverse) indexes = indexes.Reverse();

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    foreach (var i in indexes)
                    {
                        compFile.WriteBytes(i * itemSize, writeArray, 0, itemSize);
                    }
                }

                var readInto = new byte[itemSize];

                using (var compFile = CreateRealCompositeRawFile(fileChunkSize))
                {
                    foreach (var i in indexes)
                    {
                        if (i == 62)
                        {
                            var j = 0;
                        }
                        compFile.ReadBytes(i * itemSize, readInto, 0, itemSize);

                        for (int j = 0; j < itemSize; j++)
                        {
                            Assert.That(readInto[j], Is.EqualTo(writeArray[j]));
                        }
                    }
                }
            }
            finally
            {
                ClearTempFile();
            }
        }

        private void ClearTempFile()
        {
            try
            {
                File.Delete("testfile.d");
            }
            catch (Win32Exception)
            {
                return;
            }
        }

        private CompositeRawFile CreateRealCompositeRawFile(int fileChunkSize)
        {
            var bitHint = (int)Math.Ceiling(Math.Log(checked(fileChunkSize), 2));

            var f = new CompositeRawFile("testfile.d", bitHint,
               new CompositeFileFactory(), EFileAccess.ReadWrite, 0, 0, 0, EDataType.Data, 1);
            return f;
        }

        private CompositeRawFile CreateCompositeRawFile(long fileLen, int fileChunkSize)
        {
            var bitHint = (int)Math.Ceiling(Math.Log(checked(fileChunkSize), 2));
            _copositeFileFactory = new Mock<ICompositeFileFactory>();
            _compositeFile = new Mock<ICompositeFile>();
            _copositeFileFactory.Setup(c => c.OpenFile(It.IsAny<string>(), bitHint, EFileAccess.Read))
                .Returns(_compositeFile.Object);
            _compositeFile.Setup(m => m.CheckSize()).Returns(fileLen);

            var lengthFilePart = new Mock<IRawFilePart>();
            lengthFilePart.Setup(p => p.ReadInt64(0)).Returns(fileLen);
            _compositeFile.Setup(cf => cf.CreateViewAccessor(0, 8)).Returns(lengthFilePart.Object);

            var f = new CompositeRawFile("testfile.d", bitHint, 
                _copositeFileFactory.Object, EFileAccess.Read, 0, 0, 0, EDataType.Data, 1);
            return f;
        }

    }
}