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
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Storage.Serializer;

namespace Apaf.NFSdb.Core.Configuration
{
    public class JournalBuilder
    {
        private readonly JournalElement _config;
        private bool _serializerNameSet;
        private bool _serializerInstaceSet;

        public JournalBuilder()
        {
            _config = new JournalElement();
            _config.SerializerName = MetadataConstants.POCO_SERIALIZER_NAME;
        }

        public JournalBuilder WithRecordCountHint(int recordCount)
        {
            _config.RecordHint = recordCount;
            return this;
        }

        public JournalBuilder WithPartitionBy(EPartitionType partitionType)
        {
            _config.PartitionType = partitionType;
            return this;
        }

        public JournalBuilder WithLocation(string directoryPath)
        {
            _config.DefaultPath = directoryPath;
            return this;
        }

        public JournalBuilder WithSerializerFactory(ISerializerFactory serializer)
        {
            if (_serializerNameSet)
            {
                throw new NFSdbConfigurationException("Serialzer name already set to {0}. " +
                                                      "Please use either " +
                                                      "WithSerializerFactoryName or WithSerializerFactory",
                    _config.SerializerName);
            }
            _config.SerializerInstace = serializer;
            _serializerInstaceSet = true;
            return this;
        }

        public JournalBuilder WithSerializerFactoryName(string serializer)
        {
            if (_serializerInstaceSet)
            {
                throw new NFSdbConfigurationException("Serialzer instace is already set. " +
                                                      "Please use either " +
                                                      "WithSerializerFactoryName or WithSerializerFactory");
            }
            _config.SerializerName = serializer;
            _serializerNameSet = true;
            return this;
        }

        public JournalBuilder WithSymbolColumn(string name, int hintDistinctCount, int avgSize = -1,
            int maxSize = -1)
        {
            _config.Symbols.Add(new SymbolElement
            {
                Name = name,
                HintDistinctCount = hintDistinctCount,
                AvgSize = avgSize > 0 ? avgSize : MetadataConstants.DEFAULT_SYMBOL_AVG_SIZE,
                MaxSize = maxSize > 0 ? maxSize : MetadataConstants.DEFAULT_SYMBOL_MAX_SIZE,
            });
            return this;
        }

        public JournalBuilder WithStringColumn(string name, int avgSize = -1, int maxSize = -1)
        {
            _config.Strings.Add(new StringElement
            {
                Name = name,
                AvgSize = avgSize > 0 ? avgSize : MetadataConstants.DEFAULT_SYMBOL_AVG_SIZE,
                MaxSize = maxSize > 0 ? maxSize : MetadataConstants.DEFAULT_SYMBOL_MAX_SIZE,
            });
            return this;
        }

        public JournalBuilder WithEpochDateTimeColumn(string name)
        {
            _config.DateTimes.Add(new DateTimeElement
            {
                Name = name,
                IsEpochMilliseconds = true
            });
            return this;
        }

        public JournalBuilder WithTimestampColumn(string columnName)
        {
            _config.TimestampColumn = columnName;
            return this;
        }

        public IJournal<T> ToJournal<T>(EFileAccess access = EFileAccess.ReadWrite)
        {
            var meta = new JournalMetadata<T>(_config);
            var partMan = new PartitionManager<T>(meta, access, new CompositeFileFactory());
            return new Journal<T>(meta, partMan);
        }
    }
}