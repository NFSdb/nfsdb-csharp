namespace Apaf.NFSdb.Core.Configuration
{
    public class ColumnStaticMetadata
    {
        private readonly string _name;
        private readonly int _maxLength;

        public ColumnStaticMetadata(string name, int maxLength)
        {
            _name = name;
            _maxLength = maxLength;
        }

        public int MaxLength
        {
            get { return _maxLength; }
        }

        public string Name
        {
            get { return _name; }
        }
    }
}