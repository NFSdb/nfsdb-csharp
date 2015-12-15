using System.Reflection;

namespace Apaf.NFSdb.Core.Configuration
{
    public interface IPocoClassSerializerMetadata : IClassColumnSerializerMetadata
    {
        FieldInfo GetNullableHasValueField();
        FieldInfo GetNullableValueField();
    }
}