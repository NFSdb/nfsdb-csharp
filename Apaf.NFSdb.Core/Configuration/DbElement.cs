using System.Collections.Generic;
using System.Xml.Serialization;

namespace Apaf.NFSdb.Core.Configuration
{
    [XmlRoot("db")]
    public class DbElement
    {
        [XmlElement("journal")]
        public List<JournalElement> Journals { get; set; }
    }
}