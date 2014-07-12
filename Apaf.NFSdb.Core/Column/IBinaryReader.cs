using Apaf.NFSdb.Core.Collections;

namespace Apaf.NFSdb.Core.Column
{
    public interface IBinaryReader
    {
        IArray<byte> ReadBytes(int sizeBytes);
        int ReadInt32();
        char ReadChar();
        byte ReadByte();
        long ReadInt64();
        bool ReadBool();
        double ReadDouble();
        short ReadInt16();
        ushort ReadUInt16();
    }
}