namespace Apaf.NFSdb.Core.Column
{
    public interface IBitArray
    {
        int Length { get; }
        bool IsSet(int index);
        void SetIsSet(int index, bool value);
    }
}