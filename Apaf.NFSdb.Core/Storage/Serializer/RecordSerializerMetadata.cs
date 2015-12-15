using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class RecordSerializerMetadata : IColumnSerializerMetadata
    {
        public RecordSerializerMetadata(EFieldType type, string propertyName, 
            bool nullable, int columnId, int size = 0)
        {
            DataType = type;
            PropertyName = propertyName;
            Size = size;
            Nulllable = nullable;
            ColumnId = columnId;
        }

        public EFieldType DataType { get; set; }
        public int Size { get; private set; }
        public bool Nulllable { get; private set; }
        public int ColumnId { get; set; }
        public string PropertyName { get; private set; }

        public string GetFileName()
        {
            return PropertyName.Substring(0, 1).ToLower() + PropertyName.Substring(1);
        }
    }
}