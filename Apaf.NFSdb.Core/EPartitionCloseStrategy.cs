using System;
using System.Xml.Serialization;

namespace Apaf.NFSdb.Core
{
    [Flags]
    public enum EPartitionCloseStrategy
    {
        [XmlEnum("NONE")]
        None = 0x0,

        [XmlEnum("CLOSE_FULL_PARTITIONS_ON_COMMIT")]
        CloseFullPartitionOnCommit = 0x1,

        [XmlEnum("CLOSE_FULL_PARTITIONS_ASYNCHRONOUSLY")]
        FullPartitionCloseAsynchronously = 0x2,
    }
}