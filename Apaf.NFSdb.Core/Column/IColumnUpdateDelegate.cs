namespace Apaf.NFSdb.Core.Column
{
    public interface IColumnUpdateDelegate
    {
        void SetInt32(object t, int value);
        void SetInt64(object t, long value);
        void SetIntString(object t, string value);
    }
}