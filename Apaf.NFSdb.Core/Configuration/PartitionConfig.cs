using System.Collections.Generic;
using System.Xml.Serialization;

namespace Apaf.NFSdb.Core.Configuration
{
    [XmlRoot("partition")]
    public class PartitionConfig
    {
        [XmlAttribute("partitionID")]
        public int PartitionID { get; set; }

        [XmlAttribute("recordHint")]
        public long RecordHint { get; set; }

        [XmlElement("col")]
        [XmlElement("string", typeof(StringElement))]
        [XmlElement("sym", typeof(SymbolElement))]
        [XmlElement("binary", typeof(BinaryElement))]
        public List<ColumnElement> Columns { get; set; }
    }
}