namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class QlParameter
    {
        public QlParameter(string name, object value)
        {
            Value = value;
            Name = name;
        }

        public string Name { get; private set; }
        public object Value { get; private set; }
    }
}