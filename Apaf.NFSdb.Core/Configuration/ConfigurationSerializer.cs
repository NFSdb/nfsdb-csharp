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
using System.IO;
using System.Xml.Serialization;

namespace Apaf.NFSdb.Core.Configuration
{
    public static class ConfigurationSerializer
    {
        private static readonly XmlSerializer SERIALIZER = new XmlSerializer(typeof(DbElement));
        private static readonly XmlSerializer JOURNAL_SERIALIZER = new XmlSerializer(typeof(JournalElement));
        private static readonly XmlSerializer PARTITION_SERIALIZER = new XmlSerializer(typeof(PartitionConfig));

        public static DbElement ReadConfiguration(Stream input)
        {
            return (DbElement)SERIALIZER.Deserialize(input);
        }

        public static void WriteJournalConfiguration(Stream output, JournalElement element)
        {
            JOURNAL_SERIALIZER.Serialize(output, element);
        }

        public static JournalElement ReadJournalConfiguration(Stream input)
        {
            return (JournalElement)JOURNAL_SERIALIZER.Deserialize(input);
        }

        public static void WritePartitionConfiguration(Stream output, PartitionConfig element)
        {
            PARTITION_SERIALIZER.Serialize(output, element);
        }

        public static PartitionConfig ReadPartitionConfiguration(Stream input)
        {
            return (PartitionConfig)PARTITION_SERIALIZER.Deserialize(input);
        }
    }
}