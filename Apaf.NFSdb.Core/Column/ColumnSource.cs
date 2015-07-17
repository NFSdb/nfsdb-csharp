namespace Apaf.NFSdb.Core.Column
{
    public class ColumnSource
    {
        public ColumnSource(IColumnSerializerMetadata metadata, IColumn column, int fieldID)
        {
            Column = column;
            Metadata = metadata;
            FieldID = fieldID;
        }

        public int FieldID { get; private set; }
        public IColumn Column { get; private set; }
        public IColumnSerializerMetadata Metadata { get; private set; }
    }
}