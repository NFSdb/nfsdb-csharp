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
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Threading;
using Apaf.NFSdb.Core.Column;
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

        private void CreateFileSize(int retry)
        {
            try
            {
                if (_access == EFileAccess.ReadWrite)
                {
                    var fi = new FileInfo(_fullName);
                    var size = 1 << _bitHint;
                    if (!fi.Exists)
                    {
                        using (var newFile = File.Create(fi.FullName))
                        {
                            ProcessFileFlags(newFile.SafeFileHandle);
                            newFile.SetLength(size);
                        }
                    }
                }
            }
            catch (IOException ex)
            {
                if (retry > 0)
                {
                    CreateFileSize(retry - 1);
                    return;
                }
                throw new NFSdbIOException("Unable to create file or directory for path {0}", ex, Filename);
            }
        }


        public long CheckSize()
        {
            var fi = new FileInfo(_fullName);
            if (!fi.Exists) return 0L;

            return fi.Length;
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

        public IRawFilePart CreateViewAccessor(long offset, long size)
        {
            // First chunk (0 offset).
            if (!_fileOpened)
            {
                CreateFileSize(MetadataConstants.CREATE_FILE_RETRIES);
            }

            if (_access == EFileAccess.Read)
            {
                try
                {
                    var fi = new FileInfo(_fullName);
                    if (size + offset > fi.Length)
                    {
                        throw new NFSdbInvalidReadException(
                            "Opening file {0} for reading with offset {1} and size {2}. File size is not big enough.",
                            _fullName, offset, size);
                    }
                }
                catch (FileNotFoundException)
                {
                    throw new NFSdbInvalidReadException(
                        "Opening file {0} for reading failed. File does not exist.", _fullName);
                }
            }

            // Enforce access for 0 chunk.
            var fileAccess = CalculateFileAccess();
            var fileShare = CalculateFileShare();

            return OpenFile(offset, size, fileAccess, fileShare);
        }

        private IRawFilePart OpenFile(long offset, long size, FileAccess fileAccess, FileShare fileShare)
        {
            try
            {
                return OpenFileCore(offset, size, fileAccess, fileShare);
            }
            catch (NFSdbBaseExcepton)
            {
                throw;
            }
            catch (Exception ex)
            {
                Trace.TraceWarning(
                    "Failed to open file '{0}' with mode '{1}', access '{2}' and sharing '{3}'. Error {4}",
                    _fullName, FileMode.Open, fileAccess, fileShare, ex);
                throw;
            }
        }

        private IRawFilePart OpenFileCore(long offset, long size, FileAccess fileAccess, FileShare fileShare)
        {
            using (var file = File.Open(_fullName, FileMode.Open, fileAccess, fileShare))
            {
                _fileOpened = true;
                long targetFileSize = offset + size;
                if (targetFileSize > int.MaxValue/4*3)
                {
                    if (IntPtr.Size <= 4)
                    {
                        throw new NFSdbLowAddressSpaceException(
                            "Please use 64 bit process to address > 1.5 Gb of data");
                    }
                }
                if (targetFileSize > file.Length)
                {
                    if (_access == EFileAccess.ReadWrite)
                    {
                        // It should never come here if offset is 0
                        // but be already resized.
                        file.SetLength(targetFileSize);
                    }
                    else
                    {
                        throw new NFSdbConfigurationException("Bit hint for file {0} is set incorrectly - attempt to read block beyond end of file.",
                            _fullName);
                    }
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
            return _access == EFileAccess.ReadWrite ?
                FileAccess.ReadWrite : FileAccess.Read;
        }

        public string Filename { get; private set; }
    }
}