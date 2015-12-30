namespace Apaf.NFSdb.Core.Column
{
    public class ColumnSource
    {
        public ColumnSource(IColumnMetadata metadata, IColumn column, int columnID)
        {
            Column = column;
            Metadata = metadata;
            ColumnID = columnID;
        }

        public int ColumnID { get; private set; }
        public IColumn Column { get; private set; }
        public IColumnMetadata Metadata { get; private set; }
    }
}