using System.Runtime.Serialization;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Configuration
{
    public class BinaryElement : VarLenColumnElement
    {
        public BinaryElement()
        {
            OnDeserializing(new StreamingContext());
        }

        [OnDeserializing]
        private void OnDeserializing(StreamingContext streamingContext)
        {
            AvgSize = MetadataConstants.DEFAULT_BINARY_AVG_SIZE;
            MaxSize = MetadataConstants.DEFAULT_BINARY_MAX_SIZE;
            ColumnType = EFieldType.Binary;
        }
    }
}