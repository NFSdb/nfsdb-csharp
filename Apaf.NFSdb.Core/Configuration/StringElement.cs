using System.Runtime.Serialization;
using System.Xml.Serialization;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Configuration
{
    [XmlRoot("string")]
    public class StringElement : ColumnElement
    {
        public StringElement()
        {
            OnDeserializing();
        }

        [OnDeserializing]
        private void OnDeserializing()
        {
            AvgSize = MetadataConstants.DEFAULT_STRING_AVG_SIZE;
            MaxSize = MetadataConstants.DEFAULT_STRING_MAX_SIZE;
        }

        public override EFieldType ColumnType
        {
            get { return EFieldType.String; }
        }
    }
}