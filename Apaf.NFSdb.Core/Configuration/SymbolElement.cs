using System.Runtime.Serialization;
using System.Xml.Serialization;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Configuration
{
    [XmlRoot("sym")]
    public class SymbolElement : ColumnElement
    {  
        public SymbolElement()
        {
            OnDeserializing();
        }

        [OnDeserializing]
        private void OnDeserializing()
        {
            AvgSize = MetadataConstants.DEFAULT_SYMBOL_AVG_SIZE;
            MaxSize = MetadataConstants.DEFAULT_SYMBOL_MAX_SIZE;
            HintDistinctCount = MetadataConstants.DEFAULT_DISTINCT_HINT_COUNT;
        }

        [XmlAttribute("indexed")]
        public bool Indexed { get; set; }

        [XmlAttribute("sameAs")]
        public string SameAs { get; set; }

        [XmlAttribute("hintDistinctCount")]
        public int HintDistinctCount { get; set; }

        public override EFieldType ColumnType
        {
            get { return EFieldType.Symbol; }
        }
    }
}