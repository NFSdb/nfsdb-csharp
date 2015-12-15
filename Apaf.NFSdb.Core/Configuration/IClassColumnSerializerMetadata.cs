using System;
using System.Reflection;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Configuration
{
    public interface IClassColumnSerializerMetadata : IColumnSerializerMetadata
    {
        MethodInfo GetSetMethod();
        MethodInfo GetGetMethod();
        Type GetDataType();
        string FieldName { get; }
        bool IsRefType();
    }
}