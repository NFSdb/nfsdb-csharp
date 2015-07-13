using System;

namespace Apaf.NFSdb.Tests.Columns.PocoModel
{
    public class DateTimeQuote
    {
        public DateTime Timestamp { get; set; }
        public string Sym { get; set; }
        public double? Bid { get; set; }
        public double? Ask { get; set; }
        public int BidSize { get; set; }
        public int AskSize { get; set; }
        public string Mode { get; set; }
        public string Ex { get; set; }
         
    }
}