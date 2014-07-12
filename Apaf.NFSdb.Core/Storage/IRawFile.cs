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

        void ReadBytes(long offset, byte[] array, int arrayOffset, int sizeBytes);
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