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
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Core
{
    [TestFixture]
    public class PartitionManagerTests
    {
        public class DisposableTempDir : IDisposable
        {
            private const string WORKING_DIR = "partition_tests";
            private readonly string _dirName;

            public DisposableTempDir()
            {
                _dirName = WORKING_DIR + Guid.NewGuid();
            }

            public string DirName
            {
                get { return _dirName; }
            }

            public void Dispose()
            {
                if (Directory.Exists(_dirName))
                {
                    Directory.Delete(_dirName, true);
                }
            }
        }

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

        private const int SYMBOL_PARTITION_ID = MetadataConstants.SYMBOL_PARTITION_ID;

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
                CreatePartitionManager(meta, partitionString);
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
                CreatePartitionManager(meta, partitionString);

                var defPath = meta.Settings.DefaultPath;
                var pfile = Path.Combine(defPath, MetadataConstants.PARTITION_TYPE_FILENAME);

                return File.ReadAllText(pfile);
            }
        }

        [TestCase(EPartitionType.Year, "2014|2009|2013", ExpectedResult = "2009-01-01|2013-01-01|2014-01-01")]
        [TestCase(EPartitionType.Year, "2009|2013", ExpectedResult = "2009-01-01|2013-01-01")]
        [TestCase(EPartitionType.Day, "2013-02-15|2012-01-01", ExpectedResult = "2012-01-01|2013-02-15")]
        [TestCase(EPartitionType.Month, "2013-02|2012-01", ExpectedResult = "2012-01-01|2013-02-01")]
        [TestCase(EPartitionType.Month, "2013-02|Invalid|2012-01", ExpectedResult = "2012-01-01|2013-02-01")]
        public string Should_create_partitions(EPartitionType configValue,
            string subDirsString)
        {
            using (var tempDir = new DisposableTempDir())
            {
                var meta = CreateMetadata<Quote>(configValue, tempDir.DirName);
                CreateSubDirs(subDirsString.Split('|'), tempDir.DirName);

                // Act.
                var partMan = CreatePartitionManager(meta, string.Empty);

                // Verify.
                return string.Join("|",
                    partMan.Partitions.Select(p => p.StartDate.ToString("yyyy-MM-dd")));
            }
        }

#if !OPTIMIZE
        [TestCase("i.d-4|s.d-13|s.i-8|_nulls.d-8|_tx-0", 1)]
        [TestCase("i.d-8|s.d-0|s.i-16|_nulls.d-16|_tx-0", 2)]
        [TestCase("i.d-0|s.d-0|s.i-0|_nulls.d-0|_tx-0", 0)]
        [TestCase("i.d-0|s.d-0|s.i-0|_nulls.d-8|_tx-0", 0,
            ExpectedException = typeof (NFSdbTransactionStateExcepton))]
        public void Should_read_tx_context(string fileNameOffset, int count)
        {
            using (var tempDir = new DisposableTempDir())
            {
                var stubFileF = new CompositeFileFactoryStub(fileNameOffset).Stub;
                var partMan = CreatePartitionManager<FewCols>(tempDir, stubFileF.Object,
                    EFileAccess.Read);

                // Act.
                var txLog = partMan.ReadTxLog();

                // Verify.
                Assert.That(txLog.GetRowCount(1), Is.EqualTo(count));
            }
        }

        [TestCase(19, 19)]
        [TestCase(20, 20)]
        [TestCase(0, 0)]
        public void Should_override_file_offsets_by_tx_values(long lastRowID, int count)
        {
            using (var tempDir = new DisposableTempDir())
            {
                var ff = new CompositeFileFactoryStub("i.d-8|s.d-0|s.i-16|_nulls.d-16|_tx-0");
                var partMan = CreatePartitionManager<FewCols>(tempDir, ff.Stub.Object,
                    EFileAccess.Read);

                // Override Tx.
                partMan.TransactionLog.Create(new TxRec
                {
                    JournalMaxRowID = lastRowID
                });

                // Act.
                var txLog = partMan.ReadTxLog();

                // Verify.
                Assert.That(txLog.GetRowCount(1), Is.EqualTo(count));
            }
        }


        [TestCase("i.d-4|s.d-13|s.i-8|_nulls.d-8|_tx-0", 1, false)]
        [TestCase("i.d-8|s.d-345|s.i-16|_nulls.d-16|_tx-0", 2, false)]
        [TestCase("i.d-4|s.d-13|s.i-8|_nulls.d-8|_tx-0", 1, true)]
        [TestCase("i.d-8|s.d-345|s.i-16|_nulls.d-16|_tx-0", 2, true)]
        public void Ucommited_transactions_are_not_visible_to_readers(
            string fileNameOffset, int count, bool resetTxFile)
        {
            using (var tempDir = new DisposableTempDir())
            {
                var meta = CreateMetadata<FewCols>(EPartitionType.Default,
                    tempDir.DirName);
                CreateSubDirs(new[] {"default"}, tempDir.DirName);

                var ff = new CompositeFileFactoryStub(fileNameOffset);
                var partMan = CreatePartitionManager(meta, ff.Stub.Object);
                var writeTx = partMan.ReadTxLog();
                const int partitionID = 1;
                writeTx.PartitionTx[partitionID].NextRowID++;
                
                // Reset tx file to test raw file offsets.
                if (resetTxFile) ResetAppendOffset(ff); 
                
                // Act.
                var readTx = partMan.ReadTxLog();

                // Verify.
                Assert.That(readTx.GetRowCount(partitionID), Is.EqualTo(count));
            }
        }

        [TestCase("i.d-4|s.d-13|s.i-8|_nulls.d-8|_tx-0", 1, false)]
        [TestCase("i.d-8|s.d-345|s.i-16|_nulls.d-16|_tx-0", 2, false)]
        [TestCase("i.d-4|s.d-13|s.i-8|_nulls.d-8|_tx-0", 1, true)]
        [TestCase("i.d-8|s.d-345|s.i-16|_nulls.d-16|_tx-0", 2, true)]
        public void Commited_transactions_are_visible_to_readers_same_jounal(
            string fileNameOffset, int count, bool resetTxFile)
        {
            using (var tempDir = new DisposableTempDir())
            {
                var ff = new CompositeFileFactoryStub(fileNameOffset);
                var partMan =
                    CreatePartitionManager<FewCols>(tempDir, ff.Stub.Object, EFileAccess.ReadWrite);

                var writeTx = partMan.ReadTxLog();
                const int partitionID = 1;
                writeTx.PartitionTx[partitionID].NextRowID++;
                partMan.Commit(writeTx);

                // Reset tx file to test raw file offsets.
                if (resetTxFile) ResetAppendOffset(ff); 

                // Act.
                ITransactionContext readTx = partMan.ReadTxLog();

                // Verify.
                Assert.That(readTx.GetRowCount(partitionID), Is.EqualTo(count + 1));
            }
        }

        [TestCase("i.d-4|s.d-13|s.i-8|_nulls.d-8|_tx-0", 1, false)]
        [TestCase("i.d-8|s.d-345|s.i-16|_nulls.d-16|_tx-0", 2, false)]
        [TestCase("i.d-4|s.d-13|s.i-8|_nulls.d-8|_tx-0", 1, true)]
        [TestCase("i.d-8|s.d-345|s.i-16|_nulls.d-16|_tx-0", 2, true)]
        public void Commited_transactions_are_visible_to_readers_different_jounal_instance(
            string fileNameOffset, int count, bool resetTxFile)
        {
            using (var tempDir = new DisposableTempDir())
            {
                var ff = new CompositeFileFactoryStub(fileNameOffset);
                var partMan =
                    CreatePartitionManager<FewCols>(tempDir, ff.Stub.Object, EFileAccess.ReadWrite);
                var partManRead =
                    CreatePartitionManager<FewCols>(tempDir, ff.Stub.Object, EFileAccess.Read);

                var writeTx = partMan.ReadTxLog();
                const int partitionID = 1;
                writeTx.PartitionTx[partitionID].NextRowID++;
                // i.d
                var pd = writeTx.PartitionTx[partitionID];
                pd.AppendOffset[0] += 4;
                // s.i
                pd.AppendOffset[2] += 8;
                // nulls.d
                pd.AppendOffset[3] += 8;

                partMan.Commit(writeTx);

                // Reset tx file to test raw file offsets.
                if (resetTxFile) ResetAppendOffset(ff); 

                // Act.
                var readTx = partManRead.ReadTxLog();

                // Verify.
                Assert.That(readTx.GetRowCount(partitionID), Is.EqualTo(count + 1));
            }
        }

        [TestCase("i.d-4|s.d-13|s.i-8|_nulls.d-8|_tx-0", 1, false)]
        [TestCase("i.d-8|s.d-345|s.i-16|_nulls.d-16|_tx-0", 2, false)]
        [TestCase("i.d-4|s.d-13|s.i-8|_nulls.d-8|_tx-0", 1, true)]
        [TestCase("i.d-8|s.d-345|s.i-16|_nulls.d-16|_tx-0", 2, true)]
        public void When_commit_fails_all_files_are_rolled_back(
            string fileNameOffset, int count, bool resetTxFile)
        {
            using (var tempDir = new DisposableTempDir())
            {
                var ff = new CompositeFileFactoryStub(fileNameOffset, "s.d");
                var stubFileF = ff.Stub;
                var partMan =
                    CreatePartitionManager<FewCols>(tempDir, stubFileF.Object, EFileAccess.ReadWrite);
                var partManRead =
                    CreatePartitionManager<FewCols>(tempDir, stubFileF.Object, EFileAccess.Read);

                // Simulate write.
                var writeTx = partMan.ReadTxLog();
                const int partitionID = 1;
                writeTx.PartitionTx[partitionID].NextRowID++;
                // i.d
                var pd = writeTx.PartitionTx[partitionID];
                pd.AppendOffset[0] += 4;
                // s.i
                pd.AppendOffset[2] += 8;
                // nulls.d
                pd.AppendOffset[3] += 8;

                try
                {
                    partMan.Commit(writeTx);
                }
                catch (NFSdbCommitFailedException)
                {
                    // Reset tx file to test raw file offsets.
                    if (resetTxFile) ResetAppendOffset(ff); 

                    // Act.
                    var readTx = partManRead.ReadTxLog();

                    // Verify.
                    Assert.That(readTx.GetRowCount(partitionID), Is.EqualTo(count));
                    return;
                }
                Assert.Fail("Exception expected but not thrown.");
            }
        }

        [TestCase("s.symd-13|s.symi-8|s.symr.r-8|s.symr.k-20|s.d-4|s.r-4|s.k-4|_nulls.d-8|_tx-0", 25)]
        public void Should_read_symbol_key_block_size_tx_context
            (string fileNameOffset, int blockSize)
        {
            using (var tempDir = new DisposableTempDir())
            {
                var stubFileF = new CompositeFileFactoryStub(fileNameOffset);

                var partMan = CreatePartitionManager<OneSym>(tempDir,
                    stubFileF.Stub.Object, EFileAccess.Read, new[] {"s"});

                var kData = partMan.SymbolFileStorage.AllOpenedFiles()
                    .First(f => f.Filename.EndsWith("s.symr.k"));

                // Act.
                var keyBlockOffset = 0;
                kData.WriteInt64(keyBlockOffset
                            + MetadataConstants.K_FILE_ROW_BLOCK_LEN_OFFSET, blockSize);
                var txLog = partMan.ReadTxLog();

                // Verify.
                var keyBlockSize =
                    txLog.PartitionTx[MetadataConstants.SYMBOL_PARTITION_ID].SymbolData[kData.FileID].KeyBlockSize;
                Assert.That(keyBlockSize, Is.EqualTo(blockSize));
            }
        }

        [TestCase("s.symd-13|s.symi-8|s.symr.r-8|s.symr.k-20|s.d-4|s.r-4|s.k-4|_nulls.d-8|_tx-0", 25, false)]
        [TestCase("s.symd-13|s.symi-8|s.symr.r-8|s.symr.k-20|s.d-8|s.r-4|s.k-4|_nulls.d-8|_tx-0", 25, false,
            ExpectedException = typeof(NFSdbTransactionStateExcepton))]
        [TestCase("s.symd-13|s.symi-8|s.symr.r-8|s.symr.k-20|s.d-4|s.r-4|s.k-4|_nulls.d-8|_tx-0", 25, true)]
        [TestCase("s.symd-13|s.symi-8|s.symr.r-8|s.symr.k-20|s.d-8|s.r-4|s.k-4|_nulls.d-8|_tx-0", 25, true,
            ExpectedException = typeof(NFSdbTransactionStateExcepton))]
        public void Should_commit_symbol_key_size(
            string fileNameOffset, int blockSize, bool resetTxFile)
        {
            using (var tempDir = new DisposableTempDir())
            {
                var ff = new CompositeFileFactoryStub(fileNameOffset);
                var symbols = new[] {"s"};
                var partMan = CreatePartitionManager<OneSym>(tempDir,
                    ff.Stub.Object, EFileAccess.ReadWrite, symbols);
 
                var partManRead = CreatePartitionManager<OneSym>(tempDir,
                    ff.Stub.Object, EFileAccess.Read, symbols);

                var kData = partMan.SymbolFileStorage.AllOpenedFiles()
                    .First(f => f.Filename.EndsWith("s.symr.k"));

                var firstPartition = partMan.Partitions.First();
                var isset = firstPartition.Storage.AllOpenedFiles()
                    .First(f => f.Filename.EndsWith("_nulls.d"));

                var keyBlockOffset = kData.GetAppendOffset();
                kData.WriteInt64(keyBlockOffset, blockSize);
                var writeTx = partMan.ReadTxLog();

                // Act.
                // Set new block size.
                writeTx.PartitionTx[SYMBOL_PARTITION_ID].SymbolData[kData.FileID].KeyBlockSize = blockSize + 23;
                // Bump partition to change be detected.
                writeTx.PartitionTx[firstPartition.PartitionID].AppendOffset[isset.ColumnID] += 8;
                
                partMan.Commit(writeTx);

                // Reset tx file to test raw file offsets.
                if (resetTxFile) ResetAppendOffset(ff); 

                var readTx = partManRead.ReadTxLog();

                // Verify.
                long keyBlockSize = readTx.PartitionTx[SYMBOL_PARTITION_ID]
                    .SymbolData[kData.FileID].KeyBlockSize;

                Assert.That(keyBlockSize, Is.EqualTo(blockSize + 23));
            }
        }

        [TestCase("s.symd-13|s.symi-8|s.symr.r-8|s.symr.k-20|s.d-4|s.r-4|s.k-4|_nulls.d-8|_tx-0", 25, false)]
        [TestCase("s.symd-13|s.symi-8|s.symr.r-8|s.symr.k-20|s.d-4|s.r-4|s.k-4|_nulls.d-8|_tx-0", 25, true)]
        public void Should_remove_key_block_created_flag_after_commit(
            string fileNameOffset, int blockSize, bool resetTxFile)
        {
            using (var tempDir = new DisposableTempDir())
            {
                var ff = new CompositeFileFactoryStub(fileNameOffset);
                var symbols = new[] { "s" };
                var partMan = CreatePartitionManager<OneSym>(tempDir,
                    ff.Stub.Object, EFileAccess.ReadWrite, symbols);

                var kData = partMan.SymbolFileStorage.AllOpenedFiles()
                    .First(f => f.Filename.EndsWith("s.symr.k"));

                var writeTx = partMan.ReadTxLog();
                writeTx.PartitionTx[SYMBOL_PARTITION_ID].SymbolData[kData.FileID].KeyBlockCreated = true;

                // Act.
                writeTx.PartitionTx[SYMBOL_PARTITION_ID]
                    .SymbolData[kData.FileID].KeyBlockSize = blockSize + 23;
                
                partMan.Commit(writeTx);

                // Reset tx file to test raw file offsets.
                if (resetTxFile) ResetAppendOffset(ff); 

                var readTx = partMan.ReadTxLog();

                // Verify.
                var keyBlockCreated = readTx.PartitionTx[SYMBOL_PARTITION_ID].SymbolData[kData.FileID].KeyBlockCreated;
                Assert.That(keyBlockCreated, Is.EqualTo(false));
            }
        }
#endif

        private PartitionManager<T> CreatePartitionManager<T>(DisposableTempDir dir,
            ICompositeFileFactory compositeFileFactory, EFileAccess access,
            string[] symbols = null)
        {
            CreateSubDirs(new[] {"default"}, dir.DirName);
            JournalMetadata<T> meta = CreateMetadata<T>(EPartitionType.Default, dir.DirName,
                symbols);
            return new PartitionManager<T>(meta, access, compositeFileFactory);
        }

        private PartitionManager<FewCols> CreatePartitionManager(JournalMetadata<FewCols> meta,
            ICompositeFileFactory compositeFileFactory,
            EFileAccess access = EFileAccess.ReadWrite)
        {
            return new PartitionManager<FewCols>(meta, access, compositeFileFactory);
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

        private PartitionManager<Quote> CreatePartitionManager(JournalMetadata<Quote> meta, string partitionString)
        {
            string defPath = meta.Settings.DefaultPath;

            if (!Directory.Exists(defPath))
            {
                Directory.CreateDirectory(defPath);
            }
            string pfile = Path.Combine(defPath, MetadataConstants.PARTITION_TYPE_FILENAME);
            File.WriteAllText(pfile, partitionString);

            return new PartitionManager<Quote>(meta, EFileAccess.Read, new CompositeFileFactory());
        }

        private static JournalMetadata<T> CreateMetadata<T>(EPartitionType configValue,
            string workingDir)
        {
            return CreateMetadata<T>(configValue, workingDir, null);
        }

        private static JournalMetadata<T> CreateMetadata<T>(EPartitionType configValue,
            string workingDir, string[] symbols = null)
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
                    jconf.Symbols.Add(se);
                }
            }
            var meta = new JournalMetadata<T>(jconf);
            return meta;
        }

        private static void ResetAppendOffset(CompositeFileFactoryStub ff)
        {
            ff.GetFile("_tx").CreateViewAccessor(0, 10).WriteInt64(0, 0);
        }

    }
}