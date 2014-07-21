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
        [TestCase(0x400 * 0x400, 0x400 * 20, 0, 0, 0x400 * 0x400)]
        // Reading 1 chunk
        [TestCase(0x400, 0x400 + 367, 0x400, 0x400, 0x400)]
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

#if !OPTIMIZE
        // Reading 0 chunk
        [TestCase(0x100, (long)1E9, 0, 0, 0x100)]
        [TestCase(0x400 * 0x400, (long)1E9, 0, 0, 0x400 * 0x400)]
        [TestCase(0x400 * 0x400, 0x400 * 20, 0, 0, 0x400 * 0x400)]
        // Reading 1st chunk
        [TestCase(0x400, 0x400 + 367, 0x400, 0x400, 0x400)]
        // Reading 2nd chunk
        [TestCase(0x400, (long)1E9, 0x801, 0x800, 0x400)]
        // Reading beyond the file end
        //[TestCase(0x400, 0x800 - 8, 0x801, 0x801, 0, ExpectedException = typeof(NFSdbInvalidReadException))]
        public void ShouldReadFromCorrectChunk(int recordsSize, long fileLength, long readOffset, long chunkStart, long chunkSize)
        {
            var compFile = CreateCompositeRawFile(fileLength, recordsSize);
            var chunk = new Mock<IRawFilePart>();
            chunk.Setup(c => c.ReadByte(readOffset)).Returns(23);
            _compositeFile.Setup(f => f.CreateViewAccessor(chunkStart, chunkSize)).Returns(chunk.Object);
            compFile.ReadByte(readOffset);

            chunk.Verify(f => f.ReadByte(readOffset + MetadataConstants.FILE_HEADER_LENGTH), Times.Once);
        }

        [Test]
        public void ShouldSeamPartsOnReadingBytes()
        {
            const int fileLen = 0x400;
            byte[] data = Enumerable.Range(0, fileLen).Select(i => (byte) (i%256)).ToArray();
            SetLength(data, fileLen);
            var compFile = CreateCompositeRawFile(data.Length, 0x200);

            _compositeFile.Setup(f => f.CreateViewAccessor(It.IsAny<long>(), It.IsAny<long>())).Returns(
                (long offset, long size) => new BufferBinaryReader(data));

            var readLen = 0x200 + 50;
            var read = new byte[readLen];
            compFile.ReadBytes(0, read, 0, readLen);

            for (int i = 0; i < readLen; i++)
            {
                Assert.That(read[i], Is.EqualTo(data[i+MetadataConstants.FILE_HEADER_LENGTH]), "Difference at " + i);
            }
        }
#endif

        private void SetLength(byte[] data, long fileLen)
        {
            var header = BitConverter.GetBytes(fileLen).Reverse().ToArray();
            Array.Copy(header, 0, data, 0, header.Length);
        }

        private CompositeRawFile CreateCompositeRawFile(long fileLen, int fileChunkSize)
        {
            var bitHint = (int)Math.Ceiling(Math.Log(checked(fileChunkSize), 2));
            _copositeFileFactory = new Mock<ICompositeFileFactory>();
            _compositeFile = new Mock<ICompositeFile>();
            _copositeFileFactory.Setup(c => c.OpenFile(It.IsAny<string>(), bitHint, EFileAccess.Read))
                .Returns(_compositeFile.Object);
            _compositeFile.Setup(m => m.Size).Returns(fileLen);

            var lengthFilePart = new Mock<IRawFilePart>();
            lengthFilePart.Setup(p => p.ReadInt64(0)).Returns(fileLen);
            _compositeFile.Setup(cf => cf.CreateViewAccessor(0, 8)).Returns(lengthFilePart.Object);

            var f = new CompositeRawFile("testfile.d", bitHint, 
                _copositeFileFactory.Object, EFileAccess.Read, 0, 0, 0, EDataType.Data);
            return f;
        }

    }
}