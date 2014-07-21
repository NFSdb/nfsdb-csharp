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
using System.IO.MemoryMappedFiles;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Storage
{
    [TestFixture]
    public class MemeoryMapReadWriteTests
    {
        private string _dummyfileName = "dummyfile.d";

        public void Recreate(string filename)
        {
            var fi = new FileInfo(filename);
            if (fi.Exists)
            {
                File.Delete(filename);
            } 
            using (File.Open(fi.FullName, FileMode.Create))
            {
            }
        }

        [Test]
        public void ReadOnlyViewsRejectWrites()
        {
            // Cleanup.
            Recreate(_dummyfileName);

            //Act.
            const int iteration = 3;
            MemoryMappedViewAccessor[] views = null;
            try
            {
                views = CreateMemoryMappedViews(_dummyfileName, iteration, 1024,
                    FileAccess.Read, FileShare.Read);
                for (int i = 0; i < iteration; i++)
                {
                    Assert.Throws(typeof (NotSupportedException),
                        () => views[i].Write(0, (long) i));
                }
            }
            finally
            {
                CleaUpViews(views);
            }
        }

        [TestCase(13, 1024)]
        [TestCase(3, 1024*1024)]
        public void WritesVisibleToExistingReaders(int iterations, int fileSize)
        {
            // Cleanup.
            Recreate(_dummyfileName);
            MemoryMappedViewAccessor[] views = null;
            MemoryMappedViewAccessor[] views2 = null;

            try
            {
                //Act.
                views = CreateMemoryMappedViews(_dummyfileName, iterations, fileSize);
                for (int i = 0; i < iterations; i++)
                {
                    views[i].Write(0, (long) i);
                }

                // Verify.
                views2 = CreateMemoryMappedViews(_dummyfileName, iterations, fileSize);
                for (int i = 0; i < iterations; i++)
                {
                    Assert.That(views2[i].ReadInt64(0), Is.EqualTo(i));
                }
            }
            finally
            {
                // Cleanup.
                CleaUpViews(views);
                CleaUpViews(views2);
            }
        }

        [TestCase(3, 1024)]
        public void CanFacilitateOneWriter(int iterations, int fileSize)
        {
            // Cleanup.
            Recreate(_dummyfileName);

            //Act.
            MemoryMappedViewAccessor[] reads = null;
            MemoryMappedViewAccessor[] reads2 = null;
            MemoryMappedViewAccessor[] writes = null;
            MemoryMappedViewAccessor[] writes2 = null;

            try
            {
                reads = CreateMemoryMappedViews(
                    _dummyfileName, iterations, fileSize, FileAccess.Read, FileShare.ReadWrite);

                writes = CreateMemoryMappedViews(
                    _dummyfileName, iterations, fileSize, FileAccess.ReadWrite, FileShare.Read);

                reads2 = CreateMemoryMappedViews(
                    _dummyfileName, iterations + 1, fileSize, FileAccess.Read, FileShare.ReadWrite);

                Assert.Throws(typeof (IOException),
                    () => writes2 = CreateMemoryMappedViews(_dummyfileName, iterations, fileSize,
                        FileAccess.ReadWrite, FileShare.Read));
                
                CleaUpViews(writes);
                writes2 = CreateMemoryMappedViews(
                    _dummyfileName, iterations, fileSize, FileAccess.ReadWrite, FileShare.Read);
            }
            finally
            {
                // Cleanup.
                CleaUpViews(reads);
                CleaUpViews(reads2);
                CleaUpViews(writes);
                CleaUpViews(writes2);
            }
        }

        private void CleaUpViews(MemoryMappedViewAccessor[] mmf)
        {
            if (mmf != null)
            {
                foreach (var accessor in mmf)
                {
                    accessor.Dispose();
                }
            }
        }

        private static MemoryMappedViewAccessor[] CreateMemoryMappedViews(
            string fileName,
            int iterations, int fileSize, 
            FileAccess access = FileAccess.ReadWrite,
            FileShare share = FileShare.ReadWrite)
        {
            var fi = new FileInfo(fileName);
            var views = new MemoryMappedViewAccessor[iterations];
            for (int i = 0; i < iterations; i++)
            {
                // Increase the size.
                var requiredLen = (i + 1)*fileSize;
                if (fi.Length < requiredLen)
                {
                    using (var file = File.Open(fi.FullName, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
                    {
                        file.SetLength(requiredLen);
                    }
                }

                var fileAccess = i == 0 ? access : FileAccess.ReadWrite;
                var fileShare = i == 0 ? share : FileShare.ReadWrite;
                using (var file = File.Open(fi.FullName, FileMode.Open, fileAccess, fileShare))
                {
                    var segmentEnd = (i + 1)*fileSize;
                    var segmentStart = i*fileSize;
                    Console.WriteLine("Reading from {0} to {1}, File size {2}",
                        segmentStart, segmentEnd, file.Length);

                    var mmAccess = access == FileAccess.Read
                        ? MemoryMappedFileAccess.Read
                        : MemoryMappedFileAccess.ReadWrite;

                    using (var rmmf1 = MemoryMappedFile.CreateFromFile(file, null, file.Length,
                        mmAccess, null, HandleInheritability.None, false))
                    {
                        views[i] = rmmf1.CreateViewAccessor(segmentStart, fileSize,
                            mmAccess);
                    }
                }
            }
            return views;
        }
    }
}
