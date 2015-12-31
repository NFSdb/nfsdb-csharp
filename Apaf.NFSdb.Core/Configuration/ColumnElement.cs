using System.Runtime.Serialization;
using System.Xml;
using System.Xml.Serialization;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Configuration
{
    public class ColumnElement
    {
        [OnDeserializing]
        private void OnDeserializing()
        {
            IsNull = true;
        }

        [XmlAttribute("name")]
        public string Name { get; set; }

        [XmlAttribute("type")]
        public EFieldType ColumnType { get; set; }

        [XmlAttribute("isNull")]
        public bool IsNull { get; set; }

        public bool ShouldSerilizeIsNull()
        {
            return IsNull == false;
        }

        public override string ToString()
        {
            return string.Format("{0} {1}{2}", Name, ColumnType,
                IsNull ? " NULL" : null);
        }

        [XmlAnyAttribute]
        public XmlAttribute[] Attributes { get; set; }

        [XmlAnyElement]
        public XmlElement[] Elements { get; set; }
    }
}