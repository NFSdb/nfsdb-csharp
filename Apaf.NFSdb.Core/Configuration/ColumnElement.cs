using System.Xml.Serialization;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Configuration
{
    public abstract class ColumnElement
    {
        [XmlAttribute("maxsize")]
        public int MaxSize { get; set; }

        [XmlAttribute("name")]
        public string Name { get; set; }

        [XmlAttribute("avgsize")]
        public int AvgSize { get; set; }

        [XmlIgnore]
        public abstract EFieldType ColumnType { get; }
    }
}