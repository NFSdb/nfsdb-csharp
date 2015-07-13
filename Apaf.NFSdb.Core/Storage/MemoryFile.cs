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
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Threading;
using Apaf.NFSdb.Core.Exceptions;
using Microsoft.Win32.SafeHandles;

namespace Apaf.NFSdb.Core.Storage
{
    public class MemoryFile : ICompositeFile
    {
        private readonly int _bitHint;
        private readonly EFileAccess _access;
        private readonly EFileFlags _fileFlags;
        private readonly string _fullName;
        private bool _fileOpened;

        public MemoryFile(string fileName, int bitHint, EFileAccess access, EFileFlags fileFlags)
        {
            if (fileName == null) throw new ArgumentNullException("fileName");

            Filename = fileName;
            _bitHint = bitHint;
            _access = access;
            _fileFlags = fileFlags;
            _fullName = Path.GetFullPath(fileName);
        }

        private void CheckFileSize()
        {
            try
            {
                var fi = new FileInfo(_fullName);
                var size = 1 << _bitHint;

                if (!fi.Exists)
                {
                    if (fi.Directory != null && !fi.Directory.Exists)
                    {
                        fi.Directory.Create();
                    }

                    using (var newFile = File.Create(fi.FullName))
                    {
                        ProcessFileFlags(newFile.SafeFileHandle);
                        newFile.SetLength(size);
                    }
                    Size = size;
                }
                else
                {
                    Size = fi.Length;
                }
            }
            catch (IOException ex)
            {
                throw new NFSdbIOException("Unable to create file or directory for path {0}", ex, Filename);
            }
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

        private void ProcessFileFlags(SafeFileHandle fileHandle)
        {
            if (_fileFlags == EFileFlags.Sparse)
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
        }

        public long Size { get; private set; }

        public void Dispose()
        {
        }

        public IRawFilePart CreateViewAccessor(long offset, long size)
        {
            // First chunk (0 offset).
            if (!_fileOpened)
            {
                CheckFileSize();
            }

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
            var fileAccess = CalculateFileAccess();
            var fileShare = CalculateFileShare();

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
                    try
                    {
                        return new AccessorBinaryReader(
                            rmmf1.CreateViewAccessor(offset, size, mmAccess),
                            offset,
                            size);
                    }
                    catch (IOException ex)
                    {
                        throw new NFSdbIOException("Unable to create memory mapped file" +
                                                        " with address {0}, size {1} and offset {1}. " +
                                                        "See InnerException for details.",
                                                        ex, _fullName, offset, size);
                    }
                }
            }
        }

        private FileShare CalculateFileShare()
        {
            return !_fileOpened && _access == EFileAccess.ReadWrite ?
                FileShare.Read : FileShare.ReadWrite;
        }

        private FileAccess CalculateFileAccess()
        {
            return !_fileOpened && _access == EFileAccess.Read ?
                FileAccess.Read : FileAccess.ReadWrite;
        }

        public string Filename { get; private set; }
    }
}