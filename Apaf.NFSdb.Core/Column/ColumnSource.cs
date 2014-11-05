namespace Apaf.NFSdb.Core.Column
{
    public class ColumnSource
    {
        public ColumnSource(IColumnSerializerMetadata metadata, IColumn column)
        {
            Column = column;
            Metadata = metadata;
        }

        public IColumn Column { get; private set; }
        public IColumnSerializerMetadata Metadata { get; private set; }
    }
}