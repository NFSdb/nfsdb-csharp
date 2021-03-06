﻿#region copyright
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
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Server;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using Apaf.NFSdb.Tests.Common;
using Moq;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Core
{
    [TestFixture]
    public class PartitionManagerTests
    {
        public class OneSym
        {
            public Isset __isset;
            private string _s;

            public string S
            {
                get { return _s; }
                set { _s = value; }
            }

            public struct Isset
            {
                public bool s;
            }
        }

        public class FewCols
        {
            public Isset __isset;
            private int _i;
            private string _s;

            public int I
            {
                get { return _i; }
                set { _i = value; }
            }

            public string S
            {
                get { return _s; }
                set { _s = value; }
            }

            public struct Isset
            {
                public bool i;
                public bool s;
            }
        }
        
        [TestCase(EPartitionType.Default, "DEFAULT", ExpectedResult = EPartitionType.Default)]
        [TestCase(EPartitionType.Year, "DAY", ExpectedResult = EPartitionType.Day)]
        [TestCase(EPartitionType.Month, "MONTH", ExpectedResult = EPartitionType.Month)]
        [TestCase(EPartitionType.Day, "", ExpectedResult = EPartitionType.Day)]
        public EPartitionType Should_parse_partition_type(EPartitionType configValue,
            string partitionString)
        {
            using (var tempDir = new DisposableTempDir())
            {
                var meta = CreateMetadata<Quote>(configValue, tempDir.DirName);
                var pm = CreatePartitionManager(meta, partitionString);
                var jc = new JournalCore(meta, pm);
                return meta.Settings.PartitionType;
            }
        }

        [TestCase(EPartitionType.Default, "DEFAULT", ExpectedResult = "DEFAULT")]
        [TestCase(EPartitionType.Year, "DAY", ExpectedResult = "DAY")]
        [TestCase(EPartitionType.Month, "MONTH", ExpectedResult = "MONTH")]
        [TestCase(EPartitionType.Day, "", ExpectedResult = "DAY")]
        public string Should_save_value_in_partition_type_file(EPartitionType configValue,
            string partitionString)
        {
            using (var tempDir = new DisposableTempDir())
            {
                var meta = CreateMetadata<Quote>(configValue, tempDir.DirName);
                var pm = CreatePartitionManager(meta, partitionString);
                var jc = new JournalCore(meta, pm);

                var defPath = meta.Settings.DefaultPath;
                var pfile = Path.Combine(defPath, MetadataConstants.PARTITION_TYPE_FILENAME);

                return File.ReadAllText(pfile);
            }
        }

        private PartitionManager CreatePartitionManager<T>(EPartitionType pariPartitionType,
            DisposableTempDir dir,
            ICompositeFileFactory compositeFileFactory, 
            EFileAccess access,
            string[] paritions,
            params string[] symbols)
        {
            CreateSubDirs(paritions, dir.DirName);
            JournalMetadata meta = CreateMetadata<T>(pariPartitionType, dir.DirName,
                symbols);
            var txLog = new Mock<ITxLog>();
            txLog.Setup(s => s.Get()).Returns(new TxRec { JournalMaxRowID = RowIDUtil.ToRowID(1, 10)});

            var part = new PartitionManager(meta, access, compositeFileFactory, new AsyncJournalServer(TimeSpan.FromSeconds(1)), txLog.Object);
            return part;
        }

        private PartitionManager CreatePartitionManager<T>(DisposableTempDir dir,
            ICompositeFileFactory compositeFileFactory, EFileAccess access,
            params string[] symbols)
        {
            return CreatePartitionManager<T>(EPartitionType.Default, dir, compositeFileFactory,
                access, new[] {"default"}, symbols);
        }

        private PartitionManager CreatePartitionManager(JournalMetadata meta,
            ICompositeFileFactory compositeFileFactory,
            EFileAccess access = EFileAccess.ReadWrite)
        {
            return new PartitionManager(meta, access, compositeFileFactory, new Mock<IJournalServer>().Object);
        }

        private void CreateSubDirs(string[] split, string workingDir)
        {
            if (!Directory.Exists(workingDir))
            {
                Directory.CreateDirectory(workingDir);
            }
            foreach (string subDir in split)
            {
                string path = Path.Combine(workingDir, subDir);
                Directory.CreateDirectory(path);
            }
        }

        private PartitionManager CreatePartitionManager(JournalMetadata meta, string partitionString)
        {
            string defPath = meta.Settings.DefaultPath;

            if (!Directory.Exists(defPath))
            {
                Directory.CreateDirectory(defPath);
            }
            string pfile = Path.Combine(defPath, MetadataConstants.PARTITION_TYPE_FILENAME);
            File.WriteAllText(pfile, partitionString);

            return new PartitionManager(meta, EFileAccess.ReadWrite, new CompositeFileFactory(), new Mock<IJournalServer>().Object);
        }

        private static JournalMetadata CreateMetadata<T>(EPartitionType configValue,
            string workingDir, params string[] symbols)
        {
            var jconf = new JournalElement
            {
                DefaultPath = workingDir,
                PartitionType = configValue
            };

            if (symbols != null)
            {
                foreach (string sym in symbols)
                {
                    var se = new SymbolElement
                    {
                        AvgSize = 20,
                        HintDistinctCount = 100,
                        MaxSize = 120,
                        Name = sym
                    };
                    jconf.Columns.Add(se);
                }
            }
            var meta = JournalBuilder.CreateNewJournalMetadata(jconf, typeof(T));
            return meta;
        }

        private static void ResetAppendOffset(CompositeFileFactoryStub ff)
        {
            ff.GetFile("_tx").CreateViewAccessor(0, 10).WriteInt64(0, 0);
        }

    }
}