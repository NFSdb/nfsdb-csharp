using System.Runtime.Serialization;
using System.Xml.Serialization;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Configuration
{
    [XmlRoot("binary")]
    public class BinaryElement : ColumnElement
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
        }

        public override EFieldType ColumnType
        {
            get { return EFieldType.String; }
        }
    }
}