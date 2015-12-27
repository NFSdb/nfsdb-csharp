#region copyright

/*
 * Copyright (c) 2014. APAF http://apafltd.co.uk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Reflection;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class ThriftSerializerFactory : ISerializerFactory
    {
        private Type _objectType;
        private Func<ByteArray, IFixedWidthColumn[], long, 
            IRefTypeColumn[], IReadContext, object> _readMethod;

        private Action<object, ByteArray, IFixedWidthColumn[], long, 
            IRefTypeColumn[], ITransactionContext> _writeMethod;

        private IList<IClassColumnSerializerMetadata> _allColumns;

        public IEnumerable<IColumnSerializerMetadata> Initialize(Type objectType)
        {
            if (_objectType != null)
            {
                throw new InvalidOperationException("Object type has already been initialized");
            }
            if (objectType == null)
            {
                throw new ArgumentNullException("objectType");
            }
            _objectType = objectType;
            _allColumns = ParseColumnsImpl();

            return _allColumns;
        }

        private IList<IClassColumnSerializerMetadata> ParseColumnsImpl()
        {
            // Properties.
            // Public.
            IEnumerable<PropertyInfo> properties =
                _objectType.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            // Build.
            var cols = new List<IClassColumnSerializerMetadata>();
            foreach (PropertyInfo property in properties)
            {
                var propertyName = property.Name;

                // Type.
                if (property.PropertyType == typeof(byte))
                {
                    cols.Add(new ColumnSerializerMetadata(EFieldType.Byte, propertyName, GetFieldName(propertyName)));
                }
                else if (property.PropertyType == typeof(bool))
                {
                    cols.Add(new ColumnSerializerMetadata(EFieldType.Bool, propertyName, GetFieldName(propertyName)));
                }
                else if (property.PropertyType == typeof(short))
                {
                    cols.Add(new ColumnSerializerMetadata(EFieldType.Int16, propertyName, GetFieldName(propertyName)));
                }
                else if (property.PropertyType == typeof(int))
                {
                    cols.Add(new ColumnSerializerMetadata(EFieldType.Int32, propertyName, GetFieldName(propertyName)));
                }
                else if (property.PropertyType == typeof(long))
                {
                    cols.Add(new ColumnSerializerMetadata(EFieldType.Int64, propertyName, GetFieldName(propertyName)));
                }
                else if (property.PropertyType == typeof(double))
                {
                    cols.Add(new ColumnSerializerMetadata(EFieldType.Double, propertyName, GetFieldName(propertyName)));
                }
                else if (property.PropertyType == typeof(string))
                {
                    cols.Add(new ColumnSerializerMetadata(EFieldType.String, propertyName, GetFieldName(propertyName)));
                }
                else if (property.PropertyType == typeof(byte[]))
                {
                    cols.Add(new ColumnSerializerMetadata(EFieldType.Binary, propertyName, GetFieldName(propertyName)));
                }
                else
                {
                    throw new NFSdbConfigurationException("Unsupported property type " +
                        property.PropertyType);
                }
            }

            var issetField = _objectType.GetField(MetadataConstants.THRIFT_ISSET_FIELD_NAME);
            if (issetField.FieldType.Name.EndsWith(MetadataConstants.THRIFT_ISSET_FIELD_TYPE_SUFFIX))
            {
                cols.Add(new ColumnSerializerMetadata(EFieldType.BitSet, MetadataConstants.NULLS_FILE_NAME,
                    MetadataConstants.THRIFT_ISSET_FIELD_NAME));
            }
            return cols;
        }

        private string GetFieldName(string propertyName)
        {
            return "_" + propertyName.Substring(0, 1).ToLower() + propertyName.Substring(1);
        }

        public IFieldSerializer CreateFieldSerializer(IEnumerable<ColumnSource> columns)
        {
            var colsList = columns as IList<ColumnSource> ?? columns.ToList();
            if (_readMethod == null || _writeMethod == null)
            {
                _readMethod = GenerateFillMethod(colsList);
                _writeMethod = GenerateWriteMethod(colsList);
            }
            return new ObjectSerializer(colsList, _readMethod, _writeMethod);
        }

        public Func<T, TRes> ColumnReader<T, TRes>(IColumnSerializerMetadata column)
        {
            var classCol = (IClassColumnSerializerMetadata) column;
            return ReflectionHelper.CreateFieldsAccessDelegate<T, TRes>(classCol.FieldName);
        }

        /*
        .method public hidebysig static void  WriteItem(object obj,
                                                        valuetype [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray bitset,
                                                        class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn[] fixedCols,
                                                        int64 rowid,
                                                        class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn[] stringColumns,
                                                        class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext readContext) cil managed
        {
          // Code size       126 (0x7e)
          .maxstack  4
          .locals init ([0] class Apaf.NFSdb.Tests.Columns.ThriftModel.Quote q)
          IL_0000:  ldarg.0
          IL_0001:  castclass  Apaf.NFSdb.Tests.Columns.ThriftModel.Quote
          IL_0006:  stloc.0
          IL_0007:  ldarga.s   bitset
          IL_0009:  ldc.i4.0
          IL_000a:  ldloc.0
          IL_000b:  ldflda     valuetype Apaf.NFSdb.Tests.Columns.ThriftModel.Quote/Isset Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::__isset
          IL_0010:  ldfld      bool Apaf.NFSdb.Tests.Columns.ThriftModel.Quote/Isset::timestamp
          IL_0015:  ldc.i4.0
          IL_0016:  ceq
          IL_0018:  call       instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::Set(int32,
                                                                                                          bool)
          IL_001d:  ldarg.2
          IL_001e:  ldc.i4.0
          IL_001f:  ldelem.ref
          IL_0020:  ldarg.3
          IL_0021:  ldloc.0
          IL_0022:  callvirt   instance int64 Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::get_Timestamp()
          IL_0027:  ldarg.s    readContext
          IL_0029:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::SetInt64(int64,
                                                                                                                       int64,
                                                                                                                       class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
          IL_002e:  ldarga.s   bitset
          IL_0030:  ldc.i4.1
          IL_0031:  ldloc.0
          IL_0032:  ldflda     valuetype Apaf.NFSdb.Tests.Columns.ThriftModel.Quote/Isset Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::__isset
          IL_0037:  ldfld      bool Apaf.NFSdb.Tests.Columns.ThriftModel.Quote/Isset::bid
          IL_003c:  ldc.i4.0
          IL_003d:  ceq
          IL_003f:  call       instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::Set(int32,
                                                                                                          bool)
          IL_0044:  ldarg.2
          IL_0045:  ldc.i4.1
          IL_0046:  ldelem.ref
          IL_0047:  ldarg.3
          IL_0048:  ldloc.0
          IL_0049:  callvirt   instance float64 Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::get_Bid()
          IL_004e:  ldarg.s    readContext
          IL_0050:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::SetDouble(int64,
                                                                                                                        float64,
                                                                                                                        class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
          IL_0055:  ldarga.s   bitset
          IL_0057:  ldc.i4.2
          IL_0058:  ldloc.0
          IL_0059:  ldflda     valuetype Apaf.NFSdb.Tests.Columns.ThriftModel.Quote/Isset Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::__isset
          IL_005e:  ldfld      bool Apaf.NFSdb.Tests.Columns.ThriftModel.Quote/Isset::bid
          IL_0063:  ldc.i4.0
          IL_0064:  ceq
          IL_0066:  call       instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::Set(int32,
                                                                                                          bool)
          IL_006b:  ldarg.s    stringColumns
          IL_006d:  ldc.i4.0
          IL_006e:  ldelem.ref
          IL_006f:  ldarg.3
          IL_0070:  ldloc.0
          IL_0071:  callvirt   instance string Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::get_Mode()
          IL_0076:  ldarg.s    readContext
          IL_0078:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::SetValue(int64,
                                                                                                                    string,
                                                                                                                    class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
          IL_007d:  ret
        } // end of method ExpressionTests::WriteItem

*/

        private Action<object, ByteArray, IFixedWidthColumn[], long, IRefTypeColumn[], ITransactionContext> GenerateWriteMethod(IList<ColumnSource> columns)
        {
            var methodSet = typeof(ByteArray).GetMethod("Set");
            var issetType = _objectType.GetNestedType("Isset");
            var issetField = _objectType.GetField("__isset");
            var argTypes = new[]
            {
                typeof (object), typeof (ByteArray), typeof (IFixedWidthColumn[]),
                typeof (long), typeof (IRefTypeColumn[]), typeof (ITransactionContext)
            };
            var method = new DynamicMethod("WriteFromColumns" + _objectType.Name + Guid.NewGuid(), null, argTypes, GetType());
            ILGenerator il = method.GetILGenerator();
            il.DeclareLocal(_objectType);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Castclass, _objectType);
            il.Emit(OpCodes.Stloc_0);
            int fci = 0;
            int sci = 0;

            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i].Metadata;
                var columnMeta = (IClassColumnSerializerMetadata)column.SerializerMetadata;
                EFieldType fieldType = columnMeta.DataType;
                switch (fieldType)
                {
                    case EFieldType.Byte:
                    case EFieldType.Bool:
                    case EFieldType.Int16:
                    case EFieldType.Int32:
                    case EFieldType.Int64:
                    case EFieldType.Double:
                        il.Emit(OpCodes.Ldarga_S, (byte)1);
                        il.Emit(OpCodes.Ldc_I4, column.NullIndex);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldflda, issetField);
                        il.Emit(OpCodes.Ldfld, GetIssetFieldInfo(issetType, columnMeta));
                        il.Emit(OpCodes.Ldc_I4_0);
                        il.Emit(OpCodes.Ceq);
                        il.Emit(OpCodes.Call, methodSet);

                        il.Emit(OpCodes.Ldarg_2);
                        il.Emit(OpCodes.Ldc_I4, fci);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Call, _objectType.GetProperty(columnMeta.PropertyName).GetGetMethod());
                        il.Emit(OpCodes.Ldarg_S, (byte)5);
                        il.Emit(OpCodes.Callvirt, columnMeta.GetSetMethod());

                        fci++;
                        break;

                    case EFieldType.String:
                    case EFieldType.Symbol:
                        il.Emit(OpCodes.Ldarga_S, (byte)1);
                        il.Emit(OpCodes.Ldc_I4, column.NullIndex);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldflda, issetField);
                        il.Emit(OpCodes.Ldfld, GetIssetFieldInfo(issetType, columnMeta));
                        il.Emit(OpCodes.Ldc_I4_0);
                        il.Emit(OpCodes.Ceq);
                        il.Emit(OpCodes.Call, methodSet);

                        il.Emit(OpCodes.Ldarg_S, (byte)4);
                        il.Emit(OpCodes.Ldc_I4, sci);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Call, _objectType.GetProperty(columnMeta.PropertyName).GetGetMethod());
                        il.Emit(OpCodes.Ldarg_S, (byte)5);
                        il.Emit(OpCodes.Callvirt, columnMeta.GetSetMethod());

                        sci++;
                        break;

                    case EFieldType.BitSet:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            il.Emit(OpCodes.Ret);

            return (Action<object, ByteArray, IFixedWidthColumn[], long, IRefTypeColumn[], ITransactionContext>)
                method.CreateDelegate(
                    typeof(Action<object, ByteArray, IFixedWidthColumn[], long, IRefTypeColumn[], ITransactionContext>));
        }


        /*
.method public hidebysig static object  GenerateItem(valuetype [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray bitset,
                                                     class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn[] fixdCols,
                                                     int64 rowid,
                                                     class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IRefTypeColumn[] stringColumns,
                                                     class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext readContext) cil managed
{
  // Code size       122 (0x7a)
  .maxstack  4
  .locals init ([0] class Apaf.NFSdb.Tests.Columns.ThriftModel.Quote q)
  IL_0000:  newobj     instance void Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::.ctor()
  IL_0005:  stloc.0
  IL_0006:  ldarga.s   bitset
  IL_0008:  ldc.i4.0
  IL_0009:  call       instance bool [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::IsSet(int32)
  IL_000e:  brtrue.s   IL_001f
  IL_0010:  ldloc.0
  IL_0011:  ldarg.1
  IL_0012:  ldc.i4.0
  IL_0013:  ldelem.ref
  IL_0014:  ldarg.2
  IL_0015:  callvirt   instance int32 [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::GetInt32(int64)
  IL_001a:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::set_AskSize(int32)
  IL_001f:  ldarga.s   bitset
  IL_0021:  ldc.i4.1
  IL_0022:  call       instance bool [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::IsSet(int32)
  IL_0027:  brtrue.s   IL_003f
  IL_0029:  ldloc.0
  IL_002a:  ldarg.3
  IL_002b:  ldc.i4.0
  IL_002c:  ldelem.ref
  IL_002d:  ldarg.2
  IL_002e:  ldarg.s    readContext
  IL_0030:  callvirt   instance object [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IRefTypeColumn::GetValue(int64,
                                                                                                        class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext)
  IL_0035:  castclass  [mscorlib]System.String
  IL_003a:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::set_Mode(string)
  IL_003f:  ldarga.s   bitset
  IL_0041:  ldc.i4.2
  IL_0042:  call       instance bool [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::IsSet(int32)
  IL_0047:  brtrue.s   IL_0058
  IL_0049:  ldloc.0
  IL_004a:  ldarg.1
  IL_004b:  ldc.i4.1
  IL_004c:  ldelem.ref
  IL_004d:  ldarg.2
  IL_004e:  callvirt   instance int64 [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::GetInt64(int64)
  IL_0053:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::set_Timestamp(int64)
  IL_0058:  ldarga.s   bitset
  IL_005a:  ldc.i4.3
  IL_005b:  call       instance bool [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::IsSet(int32)
  IL_0060:  brtrue.s   IL_0078
  IL_0062:  ldloc.0
  IL_0063:  ldarg.3
  IL_0064:  ldc.i4.1
  IL_0065:  ldelem.ref
  IL_0066:  ldarg.2
  IL_0067:  ldarg.s    readContext
  IL_0069:  callvirt   instance object [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IRefTypeColumn::GetValue(int64,
                                                                                                        class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext)
  IL_006e:  castclass  [mscorlib]System.String
  IL_0073:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::set_Sym(string)
  IL_0078:  ldloc.0
  IL_0079:  ret
} // end of method ExpressionTests::GenerateItem


* */

        private Func<ByteArray, IFixedWidthColumn[], long, IRefTypeColumn[], IReadContext, object> GenerateFillMethod(IList<ColumnSource> columns)
        {
            ConstructorInfo constructor = _objectType.GetConstructor(Type.EmptyTypes);
            MethodInfo isSet = typeof(ByteArray).GetMethod("IsSet");

            if (constructor == null)
                throw new NFSdbConfigurationException("No default contructor found on type " + _objectType);
            var argTypes = new[]
            {
                typeof (ByteArray), typeof (IFixedWidthColumn[]),
                typeof (long), typeof (IRefTypeColumn[]), typeof (IReadContext)
            };
            var method = new DynamicMethod("ReadColumns8" + _objectType.Name + Guid.NewGuid().ToString().Substring(10),
                MethodAttributes.Static | MethodAttributes.Public, CallingConventions.Standard,
                _objectType, argTypes, _objectType, true);

            ILGenerator il = method.GetILGenerator();
            Label[] notSetLabels = columns.Select(c => il.DefineLabel()).ToArray();
            il.DeclareLocal(_objectType);
            il.Emit(OpCodes.Newobj, constructor);
            il.Emit(OpCodes.Stloc_0);

            int fci = 0;
            int sci = 0;
            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i].Metadata;
                var columnMeta = (IClassColumnSerializerMetadata)column.SerializerMetadata;
                if (columnMeta.DataType != EFieldType.BitSet)
                {
                    if (columnMeta.IsRefType())
                    {
                        il.Emit(OpCodes.Ldarga_S, (byte) 0);
                        il.Emit(OpCodes.Ldc_I4, i);
                        il.Emit(OpCodes.Call, isSet);
                        il.Emit(OpCodes.Brtrue_S, notSetLabels[i]);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Ldc_I4, sci);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_2);
                        il.Emit(OpCodes.Ldarg_S, (byte) 4);
                        il.Emit(OpCodes.Callvirt, columnMeta.GetGetMethod());
                        il.Emit(OpCodes.Castclass, columnMeta.GetDataType());
                        il.Emit(OpCodes.Callvirt, _objectType.GetProperty(columnMeta.PropertyName).GetSetMethod());

                        il.MarkLabel(notSetLabels[i]);

                        sci++;
                    }
                    else
                    {
                        il.Emit(OpCodes.Ldarga_S, (byte) 0);
                        il.Emit(OpCodes.Ldc_I4, i);
                        il.Emit(OpCodes.Call, isSet);
                        il.Emit(OpCodes.Brtrue_S, notSetLabels[i]);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldarg_1);
                        il.Emit(OpCodes.Ldc_I4, fci);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_2);
                        il.Emit(OpCodes.Callvirt, columnMeta.GetGetMethod());
                        il.Emit(OpCodes.Callvirt, _objectType.GetProperty(columnMeta.PropertyName).GetSetMethod());

                        il.MarkLabel(notSetLabels[i]);

                        fci++;
                    }
                }
            }
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ret);

            return (Func<ByteArray, IFixedWidthColumn[], long, IRefTypeColumn[], IReadContext, object>)
                method.CreateDelegate(
                    typeof(Func<ByteArray, IFixedWidthColumn[], long, IRefTypeColumn[], IReadContext, object>));
        }

        private static FieldInfo GetIssetFieldInfo(Type issetType, IColumnSerializerMetadata field)
        {
            return issetType.GetField(field.GetFileName(), BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        }
    }
}