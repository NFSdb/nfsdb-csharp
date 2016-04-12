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
using System.Threading;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;

namespace Apaf.NFSdb.Core.Storage
{
    public sealed class ColumnStorage : IColumnStorage, IDisposable
    {
        private readonly ICompositeFileFactory _compositeFileFactory;
        private readonly IRawFile[] _openedFiles;
        private readonly string _folder;
        private readonly EFileAccess _access;
        private readonly int _partitionID;

        public ColumnStorage(
            IJournalMetadata metadata,
            string folder,
            EFileAccess access,
            int partitionID,
            ICompositeFileFactory compositeFileFactory)
        {
            _access = access;
            _partitionID = partitionID;
            _compositeFileFactory = compositeFileFactory;
            _folder = folder;
            _openedFiles = new IRawFile[metadata.FileCount];
        }

        public int OpenFileCount
        {
            get { return _openedFiles.Length; }
        }

        private IRawFile Open(string filename, int fileID, int columnID, EDataType dataType, int bitHint)
        {
            string fullName = Path.Combine(_folder, filename);
            return new CompositeRawFile(fullName, bitHint, _compositeFileFactory, _access, _partitionID, fileID, columnID, dataType);
        }

        public void CloseFiles()
        {
            for (int i = 0; i < _openedFiles.Length; i++)
            {
                var file = _openedFiles[i];
                _openedFiles[i] = null;
                Thread.MemoryBarrier();

                if (file != null)
                {
                    file.Dispose();
                }
            }
        }

        public void Dispose()
        {
            CloseFiles();
        }

        public IRawFile GetFile(IColumnMetadata column, int fileID, EDataType dataType, long recordHint)
        {
            var fieldName = column.FileName;
            var filename = fieldName + GetExtension(dataType);

            Thread.MemoryBarrier();
            IRawFile file = _openedFiles[fileID];
            if (file == null)
            {
                // Size of read chunks. Rounded to power of 2.
                var bitHint = GetBitHint(dataType, recordHint, column.HintDistinctCount, column.AvgSize);

                file = Open(filename, fileID, column.ColumnID, dataType, bitHint);
                _openedFiles[fileID] = file;
            }
            return file;
        }

        private int GetBitHint(EDataType dataType, long recordHint, int distinctHint, int avgCount) 
        {
            int avgRecSize;
            var recordCount = recordHint;
            var columnDistinctCount = Math.Max(distinctHint, 1);
            if (avgCount < 0) avgCount = MetadataConstants.DEFAULT_AVG_RECORD_SIZE;

            switch (dataType)
            {
                case EDataType.Data:
                    avgRecSize = avgCount;
                    break;

                case EDataType.Symi:
                    recordCount = columnDistinctCount;
                    avgRecSize = MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE;
                    break;
                    
                case EDataType.Index:
                    avgRecSize = MetadataConstants.STRING_INDEX_FILE_RECORD_SIZE;
                    break;

                case EDataType.Symrr:
                    recordCount = columnDistinctCount;
                    avgRecSize = 8;
                    break;

                case EDataType.Datar:
                    avgRecSize = 8;
                    break;

                case EDataType.Symrk:
                case EDataType.Datak:
                    avgRecSize = 16;
                    recordCount = MetadataConstants.HASH_FUNCTION_GROUPING_RATE * MetadataConstants.AVG_KEYBLOCKS_IN_K_FILE;
                    break;

                case EDataType.Symd:
                    avgRecSize = avgCount;
                    recordCount = columnDistinctCount;
                    break;

                default:
                    throw new ArgumentOutOfRangeException("dataType");
            }
            var bitHint = CalculateHint(avgRecSize, recordCount);
            if (dataType == EDataType.Data)
            {
                bitHint = Math.Max(bitHint, MetadataConstants.MIN_FILE_BIT_HINT);
            }
            else
            {
                bitHint = Math.Max(bitHint, MetadataConstants.MIN_FILE_BIT_HINT_NON_DATA);
            }

            if (bitHint > MetadataConstants.MAX_FILE_BIT_HINT)
            {
                bitHint = MetadataConstants.MAX_FILE_BIT_HINT;
            }
            return bitHint;
        }

        private static int CalculateHint(int avgSize, long recordHint)
        {
            var bitHint = (int)Math.Floor(Math.Log(checked(avgSize * recordHint), 2));
            // Take 1/4 of the best size for better space utilization.
            // This is power of 2 - reduce by 2.
            return bitHint - 2;
        }

        public IRawFile GetOpenedFileByID(int fileID)
        {
            return _openedFiles[fileID];
        }

        private static string GetExtension(EDataType dataType)
        {
            switch (dataType)
            {
                case EDataType.Data:
                    return MetadataConstants.FILE_EXTENSION_DATA;
                case EDataType.Index:
                    return MetadataConstants.FILE_EXTENSION_INDEX;
                case EDataType.Symd:
                    return MetadataConstants.FILE_EXTENSION_SYMD;
                case EDataType.Symi:
                    return MetadataConstants.FILE_EXTENSION_SYMI;
                case EDataType.Symrk:
                    return MetadataConstants.FILE_EXTENSION_SYMRK;
                case EDataType.Symrr:
                    return MetadataConstants.FILE_EXTENSION_SYMRR;
                case EDataType.Datak:
                    return MetadataConstants.FILE_EXTENSION_DATAK;
                case EDataType.Datar:
                    return MetadataConstants.FILE_EXTENSION_DATAR;
                default:
                    throw new ArgumentOutOfRangeException("dataType");
            }
        }
    }
}