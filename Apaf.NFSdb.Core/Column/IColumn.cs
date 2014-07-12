namespace Apaf.NFSdb.Core.Column
{
    public interface IColumn
    {
        EFieldType FieldType { get; }
        string PropertyName { get; }
    }
}