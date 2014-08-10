namespace Apaf.NFSdb.Tests.Columns.PocoModel
{
    public class Trade
    {
        public long Timestamp { get; set; }
        public string Sym { get; set; }
        public double Price { get; set; }
        public int Size { get; set; }
        public int Stop { get; set; }
        public string Cond { get; set; }
        public string Ex { get; set; }
    }
}