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

using System;
using System.IO;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Server;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Storage.Serializer;

namespace Apaf.NFSdb.Core.Configuration
{
    public class JournalBuilder
    {
        private JournalElement _config;
        private bool _serializerNameSet;
        private bool _serializerInstaceSet;
        private IJournalServer _server;
        private EFileAccess _access;
        private TimeSpan _serverTasksLatency = TimeSpan.FromMilliseconds(MetadataConstants.DEFAULT_OPEN_PARTITION_TTL);

        public JournalBuilder()
        {
            _access = EFileAccess.ReadWrite;
            _config = new JournalElement();
            _config.SerializerName = MetadataConstants.POCO_SERIALIZER_NAME;
        }

        public JournalBuilder(JournalElement configuration) 
        {
            _access = EFileAccess.ReadWrite;
            _config = configuration;
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
            _config.Columns.Add(new SymbolElement
            {
                Name = name,
                HintDistinctCount = hintDistinctCount,
                Indexed = true,
                AvgSize = avgSize > 0 ? avgSize : MetadataConstants.DEFAULT_SYMBOL_AVG_SIZE,
                MaxSize = maxSize > 0 ? maxSize : MetadataConstants.DEFAULT_SYMBOL_MAX_SIZE,
            });
            return this;
        }

        public JournalBuilder WithUnindexedSymbolColumn(string name, int hintDistinctCount, int avgSize = -1,
            int maxSize = -1)
        {
            _config.Columns.Add(new SymbolElement
            {
                Name = name,
                HintDistinctCount = hintDistinctCount,
                Indexed = false,
                AvgSize = avgSize > 0 ? avgSize : MetadataConstants.DEFAULT_SYMBOL_AVG_SIZE,
                MaxSize = maxSize > 0 ? maxSize : MetadataConstants.DEFAULT_SYMBOL_MAX_SIZE,
            });
            return this;
        }

        public JournalBuilder WithStringColumn(string name, int avgSize = -1, int maxSize = -1)
        {
            _config.Columns.Add(new StringElement
            {
                Name = name,
                AvgSize = avgSize > 0 ? avgSize : MetadataConstants.DEFAULT_SYMBOL_AVG_SIZE,
                MaxSize = maxSize > 0 ? maxSize : MetadataConstants.DEFAULT_SYMBOL_MAX_SIZE,
            });
            return this;
        }

        public JournalBuilder WithEpochDateTimeColumn(string name)
        {
            _config.Columns.Add(new ColumnElement
            {
                Name = name,
                ColumnType = EFieldType.DateTimeEpochMs
            });
            return this;
        }

        public JournalBuilder WithTimestampColumn(string columnName)
        {
            _config.TimestampColumn = columnName;
            return this;
        }

        public JournalBuilder WithJournalServer(IJournalServer server)
        {
            _server = server;
            return this;
        }

        public JournalBuilder WithAccess(EFileAccess access)
        {
            _access = access;
            return this;
        }

        public JournalBuilder WithServerTasksLatency(TimeSpan latency)
        {
            _serverTasksLatency = latency;
            return this;
        }

        public JournalBuilder WithOpenPartitionTtl(TimeSpan offloadTime)
        {
            _config.OpenPartitionTtl = offloadTime.Milliseconds;
            return this;
        }

        public JournalBuilder WithFileFlags(EFileFlags fileFlags)
        {
            _config.FileFlags = fileFlags;
            return this;
        }


        public IJournalCore ToJournal()
        {
            var meta = CreateJournalMetadata(_config);
            var fileFactory = new CompositeFileFactory(_config.FileFlags);

            if (_server != null)
            {
                var partMan = new PartitionManager(meta, _access, fileFactory, _server);
                return new JournalCore(meta, partMan);
            }
            else
            {
                var server = new AsyncJournalServer(_serverTasksLatency);
                var partMan = new PartitionManager(meta, _access, fileFactory, server);
                partMan.OnDisposed += server.Dispose;
                return new JournalCore(meta, partMan);
            }
        }

        public static JournalMetadata CreateNewJournalMetadata(JournalElement config, Type t = null)
        {
            if (t != null)
            {
                var serializerFactory =
                    JournalSerializerRegistry.Instance.GetSerializer(config.SerializerName ??
                                                              MetadataConstants.DEFAULT_SERIALIZER_NAME);
                return new JournalMetadata(config, serializerFactory, t);
            }

            return new JournalMetadata(config, new RecordSerializerFactory(config), null);
        }

        public static JournalMetadata CreateJournalMetadata(JournalElement config, Type t = null)
        {
            config = UpdateConfiguration(config);
            return CreateNewJournalMetadata(config);
        }

        public IJournal<T> ToJournal<T>()
        {
            _config = UpdateConfiguration(_config);
            var serializerFactory =
                JournalSerializerRegistry.Instance.GetSerializer(_config.SerializerName ??
                                                          MetadataConstants.DEFAULT_SERIALIZER_NAME);
            var meta = new JournalMetadata(_config, serializerFactory, typeof(T));

            var fileFactory = new CompositeFileFactory(_config.FileFlags);

            if (_server != null)
            {
                var partMan = new PartitionManager(meta, _access, fileFactory, _server);
                return new Journal<T>(meta, partMan, _server);
            }
            else
            {
                var server = new AsyncJournalServer(_serverTasksLatency);
                var partMan = new PartitionManager(meta, _access, fileFactory, server);
                partMan.OnDisposed += server.Dispose;
                return new Journal<T>(meta, partMan, _server);
            }
        }

        private static JournalElement UpdateConfiguration(JournalElement conf)
        {
            string settingsFile = Path.Combine(conf.DefaultPath, MetadataConstants.JOURNAL_SETTINGS_FILE_NAME);
            JournalElement existingConfig = null;
            if (File.Exists(settingsFile))
            {
                try
                {
                    using (var dbXml = File.OpenRead(settingsFile))
                    {
                        existingConfig = ConfigurationSerializer.ReadJournalConfiguration(dbXml);
                    }
                }
                catch (IOException)
                {
                }
                catch (InvalidOperationException)
                {
                }

                if (existingConfig != null)
                {
                    existingConfig.OpenPartitionTtl = conf.OpenPartitionTtl;
                    existingConfig.MaxOpenPartitions = conf.MaxOpenPartitions;
                    existingConfig.LagHours = conf.LagHours;
                    existingConfig.DefaultPath = conf.DefaultPath;
                    existingConfig.SerializerName = conf.SerializerName;
                    existingConfig.FromDisk = true;
                    return existingConfig;
                }
            }
            return conf;
        }
    }
}