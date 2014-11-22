#region copyright
/*
 * Copyright (c) 2014. APAF http://apafltd.co.uk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Xml.Serialization;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage.Serializer;

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
            Binaries = new List<BinaryElement>();
            DateTimes = new List<DateTimeElement>();
            SerializerName = MetadataConstants.THRIFT_SERIALIZER_NAME;
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

        [XmlElement("datetime")]
        public List<DateTimeElement> DateTimes { get; set; }

        [XmlElement("binary")]
        public List<BinaryElement> Binaries { get; set; }

        [XmlElement("serializerName")]
        public string SerializerName { get; set; }

        [XmlIgnore]
        public ISerializerFactory SerializerInstace { get; set; }
    }
}