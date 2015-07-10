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
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Configuration
{
    [TestFixture]
    public class ConfigurationReaderTests
    {
        private DbElement Parse(string config)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new StreamWriter(stream))
                {
                    writer.Write(config);
                    writer.Flush();
                    stream.Position = 0;

                    var rdr = new ConfigurationReader();
                    return rdr.ReadConfiguration(stream);
                }
            }
        }

        [Test]
        public void ShouldParseJournals()
        {
            var config =
                @"<?xml version=""1.0""?>
                  <db>
                    <journal class=""namespace.model.Model1"" defaultPath=""m1"">
                    </journal>
                    <journal class=""namespace.model.Model2"">
                    </journal>
                  </db>";

            DbElement db = Parse(config);
            Assert.That(db.Journals.Count, Is.EqualTo(2));
        }

        [Test]
        public void ShouldParseSymbolsAndStrings()
        {
            var config =
                @"<?xml version=""1.0""?>
                  <db>
                    <journal class=""namespace.model.Model1"" defaultPath=""m1"">
                        <sym name=""status"" indexed=""true"" hintDistinctCount=""10""/>
                        <string name=""dest"" maxsize=""255"" avgsize=""15""/>
                        <sym name=""feed"" maxsize=""5"" hintDistinctCount=""4""/>
                    </journal>
                  </db>";

            DbElement db = Parse(config);
            Assert.That(db.Journals[0].Symbols.Count, Is.EqualTo(2));
            Assert.That(db.Journals[0].Strings.Count, Is.EqualTo(1));
        }

        [TestCase("class", ExpectedResult = "namespace.model.clas1")]
        [TestCase("defaultPath", ExpectedResult = "clas1Path")]
        [TestCase("partitionType", ExpectedResult = "None")]
        [TestCase("recordHint", ExpectedResult = "30000")]
        [TestCase("openPartitionTtl", ExpectedResult = "180")]
        [TestCase("lagHours", ExpectedResult = "12")]
        [TestCase("timestampColumn", ExpectedResult = "timestamp")]
        [TestCase("key", ExpectedResult = "imo")]
        [TestCase("maxOpenPartitions", ExpectedResult = "63")]
        public string ShouldParseJournalAttributes(string attributeName)
        {
            var config =
                @"<?xml version=""1.0""?>
                  <db>
                    <journal class=""namespace.model.clas1"" defaultPath=""clas1Path"" partitionType=""NONE""
                        recordHint=""30000"" openPartitionTTL=""180"" lagHours=""12""
                        timestampColumn=""timestamp"" key=""imo"" maxOpenPartitions=""63"">
                    </journal>
                  </db>";

            var db = Parse(config);
            var journal = db.Journals[0];
            var properyName = attributeName.Substring(0, 1).ToUpper()
                              + attributeName.Substring(1);

            return typeof (JournalElement).GetProperty(properyName)
                .GetValue(journal, null).ToString();
        }

        [TestCase("class", ExpectedResult = null)]
        [TestCase("defaultPath", ExpectedResult = null)]
        [TestCase("partitionType", ExpectedResult = EPartitionType.Default)]
        [TestCase("recordHint", ExpectedResult = 1E6)]
        [TestCase("openPartitionTtl", ExpectedResult = 1000)]
        [TestCase("lagHours", ExpectedResult = 0)]
        [TestCase("timestampColumn", ExpectedResult = null)]
        [TestCase("key", ExpectedResult = null)]
        [TestCase("maxOpenPartitions", ExpectedResult = -1)]
        public object ShouldDefaultJournalAttributes(string attributeName)
        {
            var config =
                @"<?xml version=""1.0""?>
                  <db>
                    <journal />
                  </db>";

            var db = Parse(config);
            var journal = db.Journals[0];
            var properyName = attributeName.Substring(0, 1).ToUpper()
                              + attributeName.Substring(1);

            var val = typeof(JournalElement).GetProperty(properyName)
                .GetValue(journal, null);
            return val;
        }

        [TestCase("name", ExpectedResult = "dest")]
        [TestCase("maxSize", ExpectedResult = 4567)]
        [TestCase("avgSize", ExpectedResult = 15)]
        [TestCase("columnType", ExpectedResult = EFieldType.String)]
        public object ShouldReadStringAttributes(string attributeName)
        {
            var config =
                @"<?xml version=""1.0""?>
                  <db>
                    <journal>
                        <string name=""dest"" maxsize=""4567"" avgsize=""15"" 
                            indexed=""true""/>
                    </journal>
                  </db>";

            var db = Parse(config);
            var journal = db.Journals[0];
            var properyName = attributeName.Substring(0, 1).ToUpper()
                              + attributeName.Substring(1);

            var val = typeof(StringElement).GetProperty(properyName)
                .GetValue(journal.Strings[0], null);
            return val;
        }

        [TestCase("name", ExpectedResult = null)]
        [TestCase("maxSize", ExpectedResult = 255)]
        [TestCase("avgSize", ExpectedResult = 12)]
        [TestCase("columnType", ExpectedResult = EFieldType.String)]
        public object ShouldDefaultStringAttributes(string attributeName)
        {
            var config =
                @"<?xml version=""1.0""?>
                  <db><journal>
                        <string/>
                      </journal></db>";

            var db = Parse(config);
            var journal = db.Journals[0];
            var properyName = attributeName.Substring(0, 1).ToUpper()
                              + attributeName.Substring(1);

            var val = typeof(StringElement).GetProperty(properyName)
                .GetValue(journal.Strings[0], null);
            return val;
        }

        [TestCase("name", ExpectedResult = "status")]
        [TestCase("maxSize", ExpectedResult = 5)]
        [TestCase("avgSize", ExpectedResult = 4)]
        [TestCase("indexed", ExpectedResult = true)]
        [TestCase("sameAs", ExpectedResult = "sum")]
        [TestCase("hintDistinctCount", ExpectedResult = 10)]
        [TestCase("columnType", ExpectedResult = EFieldType.Symbol)]
        public object ShouldParseSymbolsAttributes(string attributeName)
        {
            var config =
                @"<?xml version=""1.0""?>
                  <db>
                    <journal class=""namespace.model.Model1"" defaultPath=""m1"">
                        <sym name=""status"" maxsize=""5"" avgsize=""4"" indexed=""true"" 
                            hintDistinctCount=""10"" sameAs=""sum""/>
                    </journal>
                  </db>";

            var db = Parse(config);
            var journal = db.Journals[0];
            var properyName = attributeName.Substring(0, 1).ToUpper()
                              + attributeName.Substring(1);

            var val = typeof(SymbolElement).GetProperty(properyName)
                .GetValue(journal.Symbols[0], null);
            return val;
        }

        [TestCase("name", ExpectedResult = null)]
        [TestCase("maxSize", ExpectedResult = 128)]
        [TestCase("avgSize", ExpectedResult = 12)]
        [TestCase("indexed", ExpectedResult = false)]
        [TestCase("sameAs", ExpectedResult = null)]
        [TestCase("hintDistinctCount", ExpectedResult = 255)]
        [TestCase("columnType", ExpectedResult = EFieldType.Symbol)]
        public object ShouldDefaultSymbolsAttributes(string attributeName)
        {
            var config =
                @"<?xml version=""1.0""?>
                  <db>
                    <journal>
                        <sym />
                    </journal>
                  </db>";

            var db = Parse(config);
            var journal = db.Journals[0];
            var properyName = attributeName.Substring(0, 1).ToUpper()
                              + attributeName.Substring(1);

            var val = typeof(SymbolElement).GetProperty(properyName)
                .GetValue(journal.Symbols[0], null);
            return val;
        }
    }
}