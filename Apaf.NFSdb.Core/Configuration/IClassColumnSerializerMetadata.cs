using System;
using System.Reflection;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Configuration
{
    public interface IClassColumnSerializerMetadata : IColumnSerializerMetadata
    {
        MethodInfo GetSetMethod();
        MethodInfo GetGetMethod();
        Type DataType { get; }
        string FieldName { get; }
        bool IsRefType();
    }
}