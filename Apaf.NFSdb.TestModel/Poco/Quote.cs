﻿namespace Apaf.NFSdb.TestModel.Poco
{
    public class Quote
    {
        public long Timestamp { get; set; }
        public string Sym { get; set; }
        public double? Bid { get; set; }
        public double? Ask { get; set; }
        public int? BidSize { get; set; }
        public int AskSize { get; set; }
        public string Mode { get; set; }
        public string Ex { get; set; }
        public string Ex2 { get; set; }
    }
}