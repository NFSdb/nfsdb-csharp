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
using System.IO;
using System.Linq;
using Apaf.NFSdb.Core.Storage;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Storage
{
    [TestFixture]
    public class MemoryFileTests
    {
        private string _dummyFileName = "dummyFile2.d";
        IRawFile[] _handlers = new IRawFile[10];

        [SetUp]
        public void TestSetUp()
        {
            ClearFile();
        }

        [TearDown]
        public void CleanUp()
        {
            foreach (var handle in _handlers)
            {
                if (handle != null)
                {
                    handle.Dispose();
                }
            }
        }

        [Test] 
        public void ReadOnlyViewRejectsWrite()
        {
            _handlers[0] = CreateCompFile(1024, EFileAccess.Read);
            Assert.Throws(typeof(AccessViolationException), () => _handlers[0].WriteInt64(0, 12));
        }


        [Test]
        public void Can_share_file_writing_and_reading()
        {
            _handlers[0] = CreateCompFile(1024, EFileAccess.ReadWrite);
            _handlers[1] = CreateCompFile(1024, EFileAccess.Read);
            for (int i = 0; i < 10; i++)
            {
                _handlers[0].Dispose();
                _handlers[0] = CreateCompFile(1024, EFileAccess.ReadWrite);
                _handlers[0].WriteInt64(0, 12);
                _handlers[1].ReadInt64(0);
            }
        }

        [Test]
        public void CanFacilitateOneWriter()
        {
            const int chunkSize = 1024;
            const int chunkCount = 4;
            _handlers[0] = CreateCompFile(chunkSize, EFileAccess.Read);
            Read(_handlers[0], chunkCount, chunkSize);
            
            _handlers[1] = CreateCompFile(chunkSize, EFileAccess.ReadWrite);
            Read(_handlers[1], chunkCount, chunkSize);
            
            _handlers[2] = CreateCompFile(chunkSize, EFileAccess.Read);
            Read(_handlers[2], chunkCount, chunkSize);
            
            _handlers[3] = CreateCompFile(chunkSize, EFileAccess.ReadWrite);

            Assert.Throws(typeof (IOException),
                () => Read(_handlers[3], chunkCount, chunkSize));
            
        }

        private void Read(IRawFile item, int chunkCount, int chunkSize)
        {
            Enumerable.Range(0, chunkCount).Select(i => item.ReadInt32(i*chunkSize)).ToArray();
        }

        [TestCase(5, 1024)]
        [TestCase(3, 2048)]
        [TestCase(3, 60*1024)]
        [TestCase(3, 64 * 1024)]
        [TestCase(3, 65 * 1024)]
        public void WritesVisibleToExistingReaders(int iterations, int chunkSize)
        {
            _handlers[0] = CreateCompFile(chunkSize, EFileAccess.ReadWrite);
            _handlers[1] = CreateCompFile(chunkSize, EFileAccess.Read);
            // Pre-create lazy readers.
            var readNums = Enumerable.Range(0, iterations)
                .Select(i => _handlers[1].ReadInt64(i * chunkSize)).ToArray();

            // Act.
            for (int i = 0; i < iterations; i++)
            {
                _handlers[0].WriteInt64(i*chunkSize, i);
            }
            //_handlers[0].Flush();

            // Verify.
            //_handlers[1].Flush();
            var readResult = Enumerable.Range(0, iterations)
                .Select(i => _handlers[1].ReadInt64(i*chunkSize)).ToArray();
            Assert.AreEqual(string.Join("|", Enumerable.Range(0, iterations)), 
                string.Join("|", readResult));
        }

        private void ClearFile()
        {
            if (File.Exists(_dummyFileName))
            {
                File.Delete(_dummyFileName);
            }
        }

        private IRawFile CreateCompFile(int chunkSize, EFileAccess readWrite)
        {
            var bitHint = (int)Math.Ceiling(Math.Log(chunkSize, 2));
            return new CompositeRawFile(_dummyFileName,
                bitHint, new CompositeFileFactory(), readWrite, 0, 0, 0, EDataType.Data, 1);
        }
    }
}