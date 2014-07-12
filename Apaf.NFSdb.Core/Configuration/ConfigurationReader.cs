using System.IO;
using System.Xml.Serialization;

namespace Apaf.NFSdb.Core.Configuration
{
    public class ConfigurationReader
    {
        private static readonly XmlSerializer SERIALIZER = new XmlSerializer(typeof(DbElement));

        public DbElement ReadConfiguration(Stream input)
        {
            return (DbElement)SERIALIZER.Deserialize(input);
        }
    }
}