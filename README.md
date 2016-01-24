NFSdb (www.nfsdb.org) [![Build status](https://ci.appveyor.com/api/projects/status/kqttxqhf4vlcjgw8?svg=true)](https://ci.appveyor.com/project/ideoma/nfsdb-csharp)
=====================

NFSdb is a low latency, high throughput embeded database. There are two binary compatible versions written entirely in Java and in C#. 
We use memory-mapped files to bring you fully persisted database with performance characteristics of in-memory cache.
The goal of this project is to make the fastest and most developer friendly database in the world.


### Getting started

In Package Manager Console
```
> Install-Package Apaf.NFSdb.Core
```

```C#
using System;
using System.Linq;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.TestShared;
using NUnit.Framework;

namespace Apaf.NFSdb.IntegrationTests
{
    public class SimpleTest
    {
        // Model.
        public class PocoQuote
        {
            public DateTime Timestamp { get; set; }
            public string Sym { get; set; }
            public double? Bid { get; set; }
            public double? Ask { get; set; }
            public int? BidSize { get; set; }
            public int AskSize { get; set; }
            public string Mode { get; set; }
            public string Ex { get; set; }
        }

        [Test]
        public void ReadWrite()
        {
            // Clean..
            Utils.ClearJournal<PocoQuote>("c:\\temp\\quote");

            // Create.
            var journal = new JournalBuilder()
                .WithRecordCountHint(1000000)
                .WithPartitionBy(EPartitionType.Day)
                .WithLocation("c:\\temp\\quote")
                .WithSymbolColumn("Sym", 20, 5, 5)
                .WithSymbolColumn("Ex", 20, 20, 20)
                .WithSymbolColumn("Mode", 20, 20, 20)
                .WithTimestampColumn("Timestamp")
                .WithAccess(EFileAccess.ReadWrite)
                .ToJournal<PocoQuote>();

            // Append.
            var start = DateTime.Now.Date.AddMonths(-1);
            using (var wr = journal.OpenWriteTx())
            {
                var quote = new PocoQuote();
                for (int i = 0; i < 1000000; i++)
                {
                    quote.Timestamp = start.AddSeconds(i);
                    quote.Bid = i*2.04;
                    quote.Bid = i;
                    quote.BidSize = i;
                    quote.Ask = i*50.09014;
                    quote.AskSize = i;
                    quote.Ex = "LXE";
                    quote.Mode = "Fast trading";
                    quote.Sym = "SYM" + i%20;
                    wr.Append(quote);
                }
                wr.Commit();
            }

            // Query.
            // Read all where Sym = "SYM11".
            using (var rdr = journal.OpenReadTx())
            {
                Console.WriteLine(rdr.Items.Where(i => i.Sym == "SYM0").Count());
            }
        }
    }
}
```


### Performance

On test rig (Intel i7-920 @ 4Ghz) NFSdb shows average read latency of 20-30ns and write latency of 60ns per column of data. Read and write use C# unsafe code manipulating directly with Memory Map files and create very little Garbage.

### License

NFSdb is available under [Apache 2.0 License] (http://www.apache.org/licenses/LICENSE-2.0.txt)

### Support

NFSdb project is being actively developed and supported. You can raise and [Issue] (https://github.com/NFSdb/nfsdb-csharp/issues) on github or join our [google group] (https://groups.google.com/forum/#!forum/nfsdb)

Please visit our official web site [www.nfsdb.org] (http://nfsdb.org).

