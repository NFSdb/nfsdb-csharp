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

namespace Apaf.NFSdb.Core.Storage
{
    public interface IRawFile : IDisposable
    {
        int PartitionID { get; }
        int FileID { get; }
        int ColumnID { get; }
        EDataType DataType { get; }
        string Filename { get; }
        EFileAccess Access { get; }
        long MappedSize { get; }

        void ReadBytes(long offset, byte[] array, int arrayOffset, int sizeBytes);
        unsafe void ReadBytes(long offset, byte* array, int arrayOffset, int sizeBytes);
        int ReadInt32(long offset);
        byte ReadByte(long offset);
        long ReadInt64(long offset);
        bool ReadBool(long offset);
        double ReadDouble(long offset);
        short ReadInt16(long offset);
        ushort ReadUInt16(long offset);

        long GetAppendOffset();
        void SetAppendOffset(long value);
        void Flush();

        void WriteBytes(long offset, byte[] array, int arrayOffset, int sizeBytes);
        unsafe void WriteBytes(long offset, byte* array, int arrayOffset, int sizeBytes);
        void WriteInt64(long offset, long value);
        void WriteInt32(long offset, int value);
        void WriteInt16(long offset, short value);
        void WriteByte(long offset, byte value);
        void WriteBool(long offset, bool value);
        void WriteDouble(long offset, double value);
        void WriteUInt16(long offset, uint value);
    }
}