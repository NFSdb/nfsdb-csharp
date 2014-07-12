using System.Xml.Serialization;

namespace Apaf.NFSdb.Core
{
    public enum EPartitionType
    {
        [XmlEnum("DAY")]
        Day,

        [XmlEnum("MONTH")]
        Month,

        [XmlEnum("YEAR")]
        Year,

        [XmlEnum("NONE")]     
        None,

        [XmlEnum("DEFAULT")]     
        Default
    }
}