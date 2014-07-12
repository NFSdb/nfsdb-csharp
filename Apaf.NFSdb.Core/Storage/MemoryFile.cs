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
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Threading;
using Apaf.NFSdb.Core.Exceptions;
using Microsoft.Win32.SafeHandles;

namespace Apaf.NFSdb.Core.Storage
{
    public class MemoryFile : ICompositeFile
    {
        private readonly EFileAccess _access;
        private readonly string _fullName;
        private bool _fileOpened;

        public MemoryFile(string fileName, int bitHint, EFileAccess access)
        {
            if (fileName == null) throw new ArgumentNullException("fileName");
            Filename = fileName;

            _access = access;
            var fi = new FileInfo(fileName);
            var size = 1 << bitHint;

            if (!fi.Exists)
            {
                if (!fi.Directory.Exists)
                {
                    fi.Directory.Create();
                }

                using (var newFile = File.Create(fi.FullName))
                {
                    MarkAsSparseFile(newFile.SafeFileHandle);
                    newFile.SetLength(size);
                }
                Size = size;
            }
            else
            {
                Size = fi.Length;
            }
            _fullName = fi.FullName;
        }

        [DllImport("Kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        private static extern bool DeviceIoControl(
            SafeFileHandle hDevice,
            int dwIoControlCode,
            IntPtr inBuffer,
            int nInBufferSize,
            IntPtr outBuffer,
            int nOutBufferSize,
            ref int pBytesReturned,
            [In] ref NativeOverlapped lpOverlapped
        );

        public static void MarkAsSparseFile(SafeFileHandle fileHandle)
        {
            int bytesReturned = 0;
            var lpOverlapped = new NativeOverlapped();
            var result =
                DeviceIoControl(
                    fileHandle,
                    590020, //FSCTL_SET_SPARSE,
                    IntPtr.Zero,
                    0,
                    IntPtr.Zero,
                    0,
                    ref bytesReturned,
                    ref lpOverlapped);
            if (result == false)
            {
                throw new Win32Exception();
            }
        }

        public long Size { get; private set; }

        public void Dispose()
        {
        }

        public IRawFilePart CreateViewAccessor(long offset, long size)
        {
            // First chunk (0 offset).
            if (offset == 0)
            {
                // File access will be enforced.
                // Pre-check the file is big enough separately.
                if (_access == EFileAccess.Read)
                {
                    var fi = new FileInfo(_fullName);
                    if (size > fi.Length)
                    {
                        using (var file = File.Open(_fullName, FileMode.Open, 
                            FileAccess.Write, FileShare.ReadWrite))
                        {
                            file.SetLength(size);
                        }
                    }
                }
            }
            // Enforce access for 0 chunk.
            var fileAccess = CalculateFileAccess(offset);
            var fileShare = CalculateFileShare(offset);

            using (var file = File.Open(_fullName, FileMode.Open, fileAccess, fileShare))
            {
                _fileOpened = true;
                long targetFileSize = offset + size;
                if (targetFileSize > int.MaxValue/4*3)
                {
                    if (IntPtr.Size <= 4)
                    {
                        throw new NFSdbLowAddressSpaceException("Please use 64 bit process to address > 1.5 Gb of data");
                    }
                }
                if (targetFileSize > file.Length)
                {
                    // It should never come here if offset is 0
                    // but be already resized.
                    file.SetLength(targetFileSize);
                }
                var mmAccess = _access == EFileAccess.Read
                        ? MemoryMappedFileAccess.Read
                        : MemoryMappedFileAccess.ReadWrite;
                
                using (var rmmf1 = MemoryMappedFile.CreateFromFile(file, null, file.Length,
                        mmAccess, null, HandleInheritability.None, false))
                {
                    return new AccessorBinaryReader(
                        rmmf1.CreateViewAccessor(offset, size, mmAccess),
                        offset, 
                        size);
                }
            }
        }

        private FileShare CalculateFileShare(long offset)
        {
            return !_fileOpened && _access == EFileAccess.ReadWrite ?
                FileShare.Read : FileShare.ReadWrite;
        }

        private FileAccess CalculateFileAccess(long offset)
        {
            return !_fileOpened && _access == EFileAccess.Read ?
                FileAccess.Read : FileAccess.ReadWrite;
        }

        public string Filename { get; private set; }
    }
}