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
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.TestModel.Model;

namespace Apaf.NFSdb.TestShared
{
    public static class Utils
    {
        public static IJournal<T> CreateJournal<T>(EFileAccess access = EFileAccess.Read, string subdir = null)
        {
            return new JournalBuilder(ReadConfig<T>())
                .WithSerializerFactoryName(MetadataConstants.THRIFT_SERIALIZER_NAME)
                .WithAccess(access)
                .ToJournal<T>();
        }

        public static JournalElement ReadConfig<T>()
        {
            using (Stream dbXml = typeof(Quote).Assembly.GetManifestResourceStream(
                "Apaf.NFSdb.TestModel.Resources.nfsdb.xml"))
            {
                var dbElement = new ConfigurationReader().ReadConfiguration(dbXml);
                var jconf = dbElement.Journals.Single(j => j.Class.Contains(typeof(T).Name));
                jconf.DefaultPath = Path.Combine(FindJournalsPath(), jconf.DefaultPath);
                return jconf;
            }
        }

        public static IJournal<T> CreateJournal<T>(JournalElement config, EFileAccess access = EFileAccess.Read)
        {
            return new JournalBuilder(ReadConfig<T>())
                    .WithSerializerFactoryName(MetadataConstants.THRIFT_SERIALIZER_NAME)
                    .ToJournal<T>();
        }

        public static void ClearJournal<T>(string folderPath = null)
        {
            using (Stream dbXml = typeof (Quote).Assembly.GetManifestResourceStream(
                "Apaf.NFSdb.TestModel.Resources.nfsdb.xml"))
            {
                if (folderPath == null)
                {
                    var dbElement = new ConfigurationReader().ReadConfiguration(dbXml);
                    var jconf = dbElement.Journals.Single(j => j.Class.EndsWith(typeof(T).Name));
                    folderPath = jconf.DefaultPath;
                }

                var path = Path.Combine(FindJournalsPath(), folderPath);
                try
                {
                    Directory.Delete(path, true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }

        public static string FindJournalsPath()
        {
            var dirInfo = new DirectoryInfo(Environment.CurrentDirectory);
            while (!dirInfo.EnumerateDirectories("journals").Any() 
                && !dirInfo.Name.Equals("journal.net"))
            {
                dirInfo = dirInfo.Parent;
            }
            var path = Path.Combine(dirInfo.FullName, "journals");
            return path;
        }

        public static PartitionData<T> CreatePartition<T>(int? recordHint = null, EFileAccess access = EFileAccess.Read)
        {
            var mmFactory = new CompositeFileFactory();
            using (var dbXml = typeof(Quote).Assembly.GetManifestResourceStream(
                "Apaf.NFSdb.TestModel.Resources.nfsdb.xml"))
            {
                var dbElement = new ConfigurationReader().ReadConfiguration(dbXml);
                var jconf = dbElement.Journals.Single(j => j.Class.EndsWith(typeof(T).Name));
                if (recordHint.HasValue)
                {
                    jconf.RecordHint = recordHint.Value;
                }
                var journalPath = Path.Combine(FindJournalsPath(), jconf.DefaultPath);
                jconf.DefaultPath = journalPath;

                var metadata = new JournalMetadata<T>(jconf);
                var startDate = new DateTime(2013, 10, 1);
                var journalStorage = new ColumnStorage(metadata.Settings, jconf.DefaultPath,
                    access, 0, mmFactory);
                metadata.InitializeSymbols(journalStorage);

                var part = new Partition<T>(
                    metadata, new CompositeFileFactory(),
                    access, startDate, 0,
                    Path.Combine(jconf.DefaultPath, "2013-10"));

                return new PartitionData<T>(part, metadata, journalStorage, journalPath);
            }
        }

    }
}