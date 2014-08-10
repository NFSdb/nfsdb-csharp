using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class PocoSerializerFactory : ISerializerFactory
    {
        private Type _objectType;
        private IList<FieldData> _allColumns;
        private FieldData[] _allDataColumns;
        private Func<ByteArray, IFixedWidthColumn[], FixedColumnNullableWrapper[], long, IStringColumn[], IReadContext, object> _readMethod;
        private Action<object, ByteArray, IFixedWidthColumn[], FixedColumnNullableWrapper[], long, IStringColumn[], ITransactionContext> _writeMethod;
        private static readonly Guid GENERATOR_MARK_GUID = Guid.NewGuid();

        public void Initialize(Type objectType)
        {
            _objectType = objectType;
            _allColumns = ParseColumnsImpl();
            _allDataColumns = _allColumns
                .Where(c => c.DataType != EFieldType.BitSet)
                .ToArray();

            _readMethod = GenerateReadMethod();
            _writeMethod = GenerateWriteMethod();
        }

        /*
        .method public hidebysig static object  ReadItemPoco(valuetype [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray bitset,
                                                             class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn[] fixedCols,
                                                             class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.Serializer.FixedColumnNullableWrapper[] nullableCols,
                                                             int64 rowid,
                                                             class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn[] stringColumns,
                                                             class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext readContext) cil managed
        {
          // Code size       139 (0x8b)
          .maxstack  4
          .locals init ([0] class Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco q)
          IL_0000:  newobj     instance void Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::.ctor()
          IL_0005:  stloc.0
          IL_0006:  ldloc.0
          IL_0007:  ldarg.1
          IL_0008:  ldc.i4.0
          IL_0009:  ldelem.ref
          IL_000a:  ldarg.3
          IL_000b:  callvirt   instance int64 [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::GetInt64(int64)
          IL_0010:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::set_Timestamp(int64)
          IL_0015:  ldloc.0
          IL_0016:  ldarg.s    stringColumns
          IL_0018:  ldc.i4.0
          IL_0019:  ldelem.ref
          IL_001a:  ldarg.3
          IL_001b:  ldarg.s    readContext
          IL_001d:  callvirt   instance string [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::GetString(int64,
                                                                                                                class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext)
          IL_0022:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::set_Sym(string)
          IL_0027:  ldloc.0
          IL_0028:  ldarg.2
          IL_0029:  ldc.i4.0
          IL_002a:  ldelem.ref
          IL_002b:  ldarg.3
          IL_002c:  ldarg.0
          IL_002d:  callvirt   instance valuetype [mscorlib]System.Nullable`1<float64> [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.Serializer.FixedColumnNullableWrapper::GetNullableDouble(int64,
                                                                                                                                                                                         valuetype [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray)
          IL_0032:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::set_Ask(valuetype [mscorlib]System.Nullable`1<float64>)
          IL_0037:  ldloc.0
          IL_0038:  ldarg.2
          IL_0039:  ldc.i4.1
          IL_003a:  ldelem.ref
          IL_003b:  ldarg.3
          IL_003c:  ldarg.0
          IL_003d:  callvirt   instance valuetype [mscorlib]System.Nullable`1<float64> [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.Serializer.FixedColumnNullableWrapper::GetNullableDouble(int64,
                                                                                                                                                                                         valuetype [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray)
          IL_0042:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::set_Bid(valuetype [mscorlib]System.Nullable`1<float64>)
          IL_0047:  ldloc.0
          IL_0048:  ldarg.1
          IL_0049:  ldc.i4.2
          IL_004a:  ldelem.ref
          IL_004b:  ldarg.3
          IL_004c:  callvirt   instance int32 [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::GetInt32(int64)
          IL_0051:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::set_BidSize(int32)
          IL_0056:  ldloc.0
          IL_0057:  ldarg.1
          IL_0058:  ldc.i4.3
          IL_0059:  ldelem.ref
          IL_005a:  ldarg.3
          IL_005b:  callvirt   instance int32 [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::GetInt32(int64)
          IL_0060:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::set_AskSize(int32)
          IL_0065:  ldloc.0
          IL_0066:  ldarg.s    stringColumns
          IL_0068:  ldc.i4.1
          IL_0069:  ldelem.ref
          IL_006a:  ldarg.3
          IL_006b:  ldarg.s    readContext
          IL_006d:  callvirt   instance string [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::GetString(int64,
                                                                                                                class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext)
          IL_0072:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::set_Mode(string)
          IL_0077:  ldloc.0
          IL_0078:  ldarg.s    stringColumns
          IL_007a:  ldc.i4.2
          IL_007b:  ldelem.ref
          IL_007c:  ldarg.3
          IL_007d:  ldarg.s    readContext
          IL_007f:  callvirt   instance string [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::GetString(int64,
                                                                                                                class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext)
          IL_0084:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::set_Ex(string)
          IL_0089:  ldloc.0
          IL_008a:  ret
        } // end of method ExpressionTests::ReadItemPoco


         * */
        private Func<ByteArray, IFixedWidthColumn[], FixedColumnNullableWrapper[],
            long, IStringColumn[], IReadContext, object>
            GenerateReadMethod()
        {
            ConstructorInfo constructor = _objectType.GetConstructor(Type.EmptyTypes);
            if (constructor == null)
            {
                throw new NFSdbConfigurationException("No default constructor found on type " + _objectType);
            }

            var argTypes = new[]
            {
                typeof (ByteArray), typeof (IFixedWidthColumn[]), typeof(FixedColumnNullableWrapper[]),
                typeof (long), typeof (IStringColumn[]), typeof (IReadContext)
            };

            var method = new DynamicMethod("Rd" + _objectType.GUID + GENERATOR_MARK_GUID,
                MethodAttributes.Static | MethodAttributes.Public, CallingConventions.Standard,
                _objectType, argTypes, _objectType, true);

            ILGenerator il = method.GetILGenerator();
            il.DeclareLocal(_objectType);
            il.Emit(OpCodes.Newobj, constructor);
            il.Emit(OpCodes.Stloc_0);

            int fci = 0;
            int sci = 0;
            int nulli  = 0;

            for (int i = 0; i < _allDataColumns.Length; i++)
            {
                var column = _allDataColumns[i];
                bool isString = column.DataType == EFieldType.String
                                || column.DataType == EFieldType.Symbol;
                
                if (column.DataType != EFieldType.BitSet)
                {
                    // Start.
                    il.Emit(OpCodes.Ldloc_0);

                    if (isString)
                    {
                        // Use IStringColumn[].
                        il.Emit(OpCodes.Ldarg_S, (byte)4);
                        il.Emit(OpCodes.Ldc_I4, sci++);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Ldarg_S, (byte)5);
                        il.Emit(OpCodes.Callvirt, column.GetGetMethod());
                    }
                    else if (!column.Nulllable)
                    {
                        // Use IFixedWidthColumn[].
                        il.Emit(OpCodes.Ldarg_1);
                        il.Emit(OpCodes.Ldc_I4, fci++);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Callvirt, column.GetGetMethod());
                    }
                    else
                    {
                        // Use FixedColumnNullableWrapper[].
                        il.Emit(OpCodes.Ldarg_2);
                        il.Emit(OpCodes.Ldc_I4, nulli++);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Ldarg_0);
                        il.Emit(OpCodes.Callvirt, GetNullableGetMethod(column));
                    }

                    il.Emit(OpCodes.Callvirt, _objectType.GetProperty(column.PropertyName).GetSetMethod());
                }
            }
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ret);

            return (Func<ByteArray, IFixedWidthColumn[], FixedColumnNullableWrapper[], 
                long, IStringColumn[], IReadContext, object>)
                method.CreateDelegate(
                    typeof(Func<ByteArray, IFixedWidthColumn[], FixedColumnNullableWrapper[],
                    long, IStringColumn[], IReadContext, object>));
        }

        /*
.method public hidebysig static void  WriteItemPoco(object obj,
                                                    valuetype [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray bitset,
                                                    class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn[] fixedCols,
                                                    class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.Serializer.FixedColumnNullableWrapper[] nullableCols,
                                                    int64 rowid,
                                                    class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn[] stringColumns,
                                                    class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext readContext) cil managed
{
  // Code size       157 (0x9d)
  .maxstack  5
  .locals init ([0] class Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco q)
  IL_0000:  ldarg.0
  IL_0001:  castclass  Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco
  IL_0006:  stloc.0
  IL_0007:  ldarg.2
  IL_0008:  ldc.i4.0
  IL_0009:  ldelem.ref
  IL_000a:  ldarg.s    rowid
  IL_000c:  ldloc.0
  IL_000d:  callvirt   instance int64 Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::get_Timestamp()
  IL_0012:  ldarg.s    readContext
  IL_0014:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::SetInt64(int64,
                                                                                                         int64,
                                                                                                         class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_0019:  ldarg.s    stringColumns
  IL_001b:  ldc.i4.0
  IL_001c:  ldelem.ref
  IL_001d:  ldarg.s    rowid
  IL_001f:  ldloc.0
  IL_0020:  callvirt   instance string Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::get_Sym()
  IL_0025:  ldarg.s    readContext
  IL_0027:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::SetString(int64,
                                                                                                      string,
                                                                                                      class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_002c:  ldarg.3
  IL_002d:  ldc.i4.0
  IL_002e:  ldelem.ref
  IL_002f:  ldarg.s    rowid
  IL_0031:  ldloc.0
  IL_0032:  callvirt   instance valuetype [mscorlib]System.Nullable`1<float64> Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::get_Bid()
  IL_0037:  ldarg.1
  IL_0038:  ldarg.s    readContext
  IL_003a:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.Serializer.FixedColumnNullableWrapper::SetNullableDouble(int64,
                                                                                                                                       valuetype [mscorlib]System.Nullable`1<float64>,
                                                                                                                                       valuetype [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray,
                                                                                                                                       class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_003f:  ldarg.3
  IL_0040:  ldc.i4.1
  IL_0041:  ldelem.ref
  IL_0042:  ldarg.s    rowid
  IL_0044:  ldloc.0
  IL_0045:  callvirt   instance valuetype [mscorlib]System.Nullable`1<float64> Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::get_Ask()
  IL_004a:  ldarg.1
  IL_004b:  ldarg.s    readContext
  IL_004d:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.Serializer.FixedColumnNullableWrapper::SetNullableDouble(int64,
                                                                                                                                       valuetype [mscorlib]System.Nullable`1<float64>,
                                                                                                                                       valuetype [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray,
                                                                                                                                       class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_0052:  ldarg.2
  IL_0053:  ldc.i4.0
  IL_0054:  ldelem.ref
  IL_0055:  ldarg.s    rowid
  IL_0057:  ldloc.0
  IL_0058:  callvirt   instance int32 Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::get_BidSize()
  IL_005d:  ldarg.s    readContext
  IL_005f:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::SetInt32(int64,
                                                                                                         int32,
                                                                                                         class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_0064:  ldarg.2
  IL_0065:  ldc.i4.1
  IL_0066:  ldelem.ref
  IL_0067:  ldarg.s    rowid
  IL_0069:  ldloc.0
  IL_006a:  callvirt   instance int32 Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::get_AskSize()
  IL_006f:  ldarg.s    readContext
  IL_0071:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::SetInt32(int64,
                                                                                                         int32,
                                                                                                         class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_0076:  ldarg.s    stringColumns
  IL_0078:  ldc.i4.1
  IL_0079:  ldelem.ref
  IL_007a:  ldarg.s    rowid
  IL_007c:  ldloc.0
  IL_007d:  callvirt   instance string Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::get_Mode()
  IL_0082:  ldarg.s    readContext
  IL_0084:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::SetString(int64,
                                                                                                      string,
                                                                                                      class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_0089:  ldarg.s    stringColumns
  IL_008b:  ldc.i4.1
  IL_008c:  ldelem.ref
  IL_008d:  ldarg.s    rowid
  IL_008f:  ldloc.0
  IL_0090:  callvirt   instance string Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::get_Ex()
  IL_0095:  ldarg.s    readContext
  IL_0097:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::SetString(int64,
                                                                                                      string,
                                                                                                      class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_009c:  ret
} // end of method ExpressionTests::WriteItemPoco


         * */
        private Action<object, ByteArray, IFixedWidthColumn[], 
            FixedColumnNullableWrapper[], long, IStringColumn[], ITransactionContext> 
            GenerateWriteMethod()
        {
            var argTypes = new[]
            {
                typeof (object), typeof (ByteArray), typeof (IFixedWidthColumn[]), 
                typeof(FixedColumnNullableWrapper[]),
                typeof (long), typeof (IStringColumn[]), typeof (ITransactionContext)
            };
            var method = new DynamicMethod("Wt" + _objectType.GUID + GENERATOR_MARK_GUID,
                MethodAttributes.Static | MethodAttributes.Public, CallingConventions.Standard,
                null, argTypes, _objectType, true);

            ILGenerator il = method.GetILGenerator();
            il.DeclareLocal(_objectType);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Castclass, _objectType);
            il.Emit(OpCodes.Stloc_0);
            int fci = 0;
            int sci = 0;
            int nci = 0;

            for (int i = 0; i < _allDataColumns.Length; i++)
            {
                var column = _allColumns[i];
                EFieldType fieldType = column.DataType;
                if (column.DataType != EFieldType.BitSet)
                {
                    if (!column.Nulllable)
                    {
                        if (fieldType == EFieldType.String || fieldType == EFieldType.Symbol)
                        {
                            il.Emit(OpCodes.Ldarg_S, (byte) 5);
                            il.Emit(OpCodes.Ldc_I4, sci++);
                        }
                        else
                        {
                            il.Emit(OpCodes.Ldarg_2);
                            il.Emit(OpCodes.Ldc_I4, fci++);
                        }
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_S, (byte) 4);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Callvirt, _objectType.GetProperty(column.PropertyName).GetGetMethod());
                        il.Emit(OpCodes.Ldarg_S, (byte) 6);
                        il.Emit(OpCodes.Callvirt, column.GetSetMethod());
                    }
                    else
                    {
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Ldc_I4, nci++);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_S, (byte) 4);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Callvirt, _objectType.GetProperty(column.PropertyName).GetGetMethod());
                        il.Emit(OpCodes.Ldarg_1);
                        il.Emit(OpCodes.Ldarg_S, (byte) 6);
                        il.Emit(OpCodes.Callvirt , GetNullableSetMethod(column));
                    }
                }
            }
            il.Emit(OpCodes.Ret);

            return (Action<object, ByteArray, IFixedWidthColumn[],
                FixedColumnNullableWrapper[], long, IStringColumn[], ITransactionContext>)
                method.CreateDelegate(
                    typeof(Action<object, ByteArray, IFixedWidthColumn[], 
                    FixedColumnNullableWrapper[], long, IStringColumn[], ITransactionContext>));

        }

        private static MethodInfo GetNullableSetMethod(FieldData column)
        {
            return typeof (FixedColumnNullableWrapper).GetMethod("SetNullable" + column.DataType.ToString());
        }

        private static MethodInfo GetNullableGetMethod(FieldData column)
        {
            return typeof(FixedColumnNullableWrapper).GetMethod("GetNullable" + column.DataType.ToString());
        }

        public IList<FieldData> ParseColumns()
        {
            return _allColumns;
        }

        public IList<FieldData> ParseColumnsImpl()
        {
            // Properties.
            // Public.
            IEnumerable<PropertyInfo> properties =
                _objectType.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            // Build.
            var cols = new List<FieldData>();
            int nullableCount = 0;

            foreach (PropertyInfo property in properties)
            {
                var propertyName = property.Name;
                
                // Type.
                var propertyType = property.PropertyType;
                bool nullable = false;
                if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    nullableCount++;
                    propertyType = propertyType.GetGenericArguments()[0];
                    nullable = true;
                }

                if (propertyType == typeof(byte))
                {
                    cols.Add(new FieldData(EFieldType.Byte, propertyName, nullable));
                }
                else if (propertyType == typeof(bool))
                {
                    cols.Add(new FieldData(EFieldType.Bool, propertyName, nullable));
                }
                else if (propertyType == typeof(short))
                {
                    cols.Add(new FieldData(EFieldType.Int16, propertyName, nullable));
                }
                else if (propertyType == typeof(int))
                {
                    cols.Add(new FieldData(EFieldType.Int32, propertyName, nullable));
                }
                else if (propertyType == typeof(long))
                {
                    cols.Add(new FieldData(EFieldType.Int64, propertyName, nullable));
                }
                else if (propertyType == typeof(double))
                {
                    cols.Add(new FieldData(EFieldType.Double, propertyName, nullable));
                }
                else if (propertyType == typeof(string))
                {
                    // ReSharper disable once RedundantArgumentDefaultValue
                    cols.Add(new FieldData(EFieldType.String, propertyName, false));
                }
                else
                {
                    throw new NFSdbConfigurationException("Unsupported property type " +
                        propertyType);
                }
            }

            if (nullableCount > 0)
            {
                cols.Add(new FieldData(EFieldType.BitSet, MetadataConstants.NULLS_FILE_NAME, false, nullableCount));
            }
            return cols;
        }

        public IFieldSerializer CreateFieldSerializer(IEnumerable<IColumn> columns)
        {
            return new PocoObjectSerializer(columns,_allDataColumns, _readMethod, _writeMethod);
        }
    }
}