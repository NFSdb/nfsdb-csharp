using System.Runtime.Serialization;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Configuration
{
    public class BinaryElement : VarLenColumnElement
    {
        public BinaryElement()
        {
            OnDeserializing();
        }

        [OnDeserializing]
        private void OnDeserializing()
        {
            AvgSize = MetadataConstants.DEFAULT_BINARY_AVG_SIZE;
            MaxSize = MetadataConstants.DEFAULT_BINARY_MAX_SIZE;
            ColumnType = EFieldType.Binary;
        }
    }
}