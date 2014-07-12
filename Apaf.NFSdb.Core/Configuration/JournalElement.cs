using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Xml.Serialization;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Configuration
{
    [XmlRoot("journal")]
    public class JournalElement
    {
        public JournalElement()
        {
            OnDeserializing();
        }

        [OnDeserializing]
        private void OnDeserializing()
        {
            PartitionType = EPartitionType.Default;
            RecordHint = MetadataConstants.DEFAULT_RECORD_HINT;
            OpenPartitionTTL = MetadataConstants.DEFAULT_OPEN_PARTITION_TTL;
            LagHours = MetadataConstants.DEFAULT_LAG_HOURS;
            MaxOpenPartitions = MetadataConstants.DEFAULT_MAX_OPEN_PARTITIONS;
            Strings = new List<StringElement>();
            Symbols = new List<SymbolElement>();
        }

        [XmlAttribute("class")]
        public string Class { get; set; }

        [XmlAttribute("defaultPath")]
        public string DefaultPath { get; set; }

        [XmlAttribute("timestampColumn")]
        public string TimestampColumn { get; set; }

        [XmlAttribute("partitionType")]
        public EPartitionType PartitionType { get; set; }

        [XmlAttribute("recordHint")]
        public int RecordHint { get; set; }

        [XmlAttribute("openPartitionTTL")]
        public int OpenPartitionTTL { get; set; }

        [XmlAttribute("lagHours")]
        public int LagHours { get; set; }

        [XmlAttribute("maxOpenPartitions")]
        public int MaxOpenPartitions { get; set; }

        [XmlAttribute("key")]
        public string Key { get; set; }

        [XmlElement("sym")]
        public List<SymbolElement> Symbols { get; set; }

        [XmlElement("string")]
        public List<StringElement> Strings { get; set; }

    }
}