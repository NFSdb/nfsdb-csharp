﻿#region copyright

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
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public class ThriftSerializerFactory : ISerializerFactory
    {
        private Type _objectType;
        private Func<ByteArray, IFixedWidthColumn[], long, 
            IStringColumn[], IReadContext, object> _readMethod;

        private Action<object, ByteArray, IFixedWidthColumn[], long, 
            IStringColumn[], ITransactionContext> _writeMethod;

        private FieldData[] _allDataColumns;
        private FieldData[] _fixedColumns;
        private FieldData[] _stringColumns;
        private IList<FieldData> _allColumns;

        public void Initialize(Type objectType)
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

            _allDataColumns = _allColumns
                .Where(c => c.DataType != EFieldType.BitSet)
                .ToArray();

            _fixedColumns = _allDataColumns
                .Where(c => c.DataType != EFieldType.BitSet
                            && c.DataType != EFieldType.String
                            && c.DataType != EFieldType.Symbol)
                .ToArray();

            _stringColumns = _allDataColumns
                .Where(c => c.DataType == EFieldType.String
                            || c.DataType == EFieldType.Symbol)
                .ToArray();

            _readMethod = GenerateFillMethod();
            _writeMethod = GenerateWriteMethod();
        }

        public IList<FieldData> ParseColumns()
        {
            return _allColumns;
        }

        private IList<FieldData> ParseColumnsImpl()
        {
            // Properties.
            // Public.
            IEnumerable<PropertyInfo> properties =
                _objectType.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            // Build.
            var cols = new List<FieldData>();

            foreach (PropertyInfo property in properties)
            {
                var propertyName = property.Name;

                // Type.
                if (property.PropertyType == typeof(byte))
                {
                    cols.Add(new FieldData(EFieldType.Byte, propertyName));
                }
                else if (property.PropertyType == typeof(bool))
                {
                    cols.Add(new FieldData(EFieldType.Bool, propertyName));
                }
                else if (property.PropertyType == typeof(short))
                {
                    cols.Add(new FieldData(EFieldType.Int16, propertyName));
                }
                else if (property.PropertyType == typeof(int))
                {
                    cols.Add(new FieldData(EFieldType.Int32, propertyName));
                }
                else if (property.PropertyType == typeof(long))
                {
                    cols.Add(new FieldData(EFieldType.Int64, propertyName));
                }
                else if (property.PropertyType == typeof(double))
                {
                    cols.Add(new FieldData(EFieldType.Double, propertyName));
                }
                else if (property.PropertyType == typeof(string))
                {
                    cols.Add(new FieldData(EFieldType.String, propertyName));
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
                cols.Add(new FieldData(EFieldType.BitSet, MetadataConstants.NULLS_FILE_NAME));
            }
            return cols;
        }

        public IFieldSerializer CreateFieldSerializer(IEnumerable<IColumn> columns)
        {
            return new ThriftObjectSerializer(columns, _readMethod, _writeMethod);
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
          IL_0078:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::SetString(int64,
                                                                                                                    string,
                                                                                                                    class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
          IL_007d:  ret
        } // end of method ExpressionTests::WriteItem

*/

        private Action<object, ByteArray, IFixedWidthColumn[],
            long, IStringColumn[], ITransactionContext> GenerateWriteMethod()
        {
            var methodSet = typeof(ByteArray).GetMethod("Set");
            var issetType = _objectType.GetNestedType("Isset");
            var issetField = _objectType.GetField("__isset");
            var argTypes = new[]
            {
                typeof (object), typeof (ByteArray), typeof (IFixedWidthColumn[]),
                typeof (long), typeof (IStringColumn[]), typeof (ITransactionContext)
            };
            var method = new DynamicMethod("WriteFromColumns" + _objectType.Name + Guid.NewGuid(), null, argTypes, GetType());
            ILGenerator il = method.GetILGenerator();
            il.DeclareLocal(_objectType);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Castclass, _objectType);
            il.Emit(OpCodes.Stloc_0);
            int fci = 0;
            int sci = 0;

            for (int i = 0; i < _allDataColumns.Length; i++)
            {
                var column = _allDataColumns[i];
                EFieldType fieldType = column.DataType;
                switch (fieldType)
                {
                    case EFieldType.Byte:
                    case EFieldType.Bool:
                    case EFieldType.Int16:
                    case EFieldType.Int32:
                    case EFieldType.Int64:
                    case EFieldType.Double:

                        var fixedCol = _fixedColumns[fci];
                        if (fixedCol != column)
                        {
                            throw new NFSdbInitializationException(
                                "Error generating Object Reader. Fixed column order does not match columns order");
                        }

                        il.Emit(OpCodes.Ldarga_S, (byte)1);
                        il.Emit(OpCodes.Ldc_I4, i);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldflda, issetField);
                        il.Emit(OpCodes.Ldfld, GetIssetFieldInfo(issetType, fixedCol.PropertyName));
                        il.Emit(OpCodes.Ldc_I4_0);
                        il.Emit(OpCodes.Ceq);
                        il.Emit(OpCodes.Call, methodSet);

                        il.Emit(OpCodes.Ldarg_2);
                        il.Emit(OpCodes.Ldc_I4, fci);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Call, _objectType.GetProperty(fixedCol.PropertyName).GetGetMethod());
                        il.Emit(OpCodes.Ldarg_S, (byte)5);
                        il.Emit(OpCodes.Callvirt, typeof(IFixedWidthColumn).GetMethod("Set" + fieldType));

                        fci++;
                        break;

                    case EFieldType.String:
                    case EFieldType.Symbol:
                        var stringColumn = _stringColumns[sci];
                        if (stringColumn != column)
                        {
                            throw new NFSdbInitializationException(
                                "Error generating Object Reader. String column order does not match all columns order");
                        }

                        il.Emit(OpCodes.Ldarga_S, (byte)1);
                        il.Emit(OpCodes.Ldc_I4, i);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldflda, issetField);
                        il.Emit(OpCodes.Ldfld, GetIssetFieldInfo(issetType, stringColumn.PropertyName));
                        il.Emit(OpCodes.Ldc_I4_0);
                        il.Emit(OpCodes.Ceq);
                        il.Emit(OpCodes.Call, methodSet);

                        il.Emit(OpCodes.Ldarg_S, (byte)4);
                        il.Emit(OpCodes.Ldc_I4, sci);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Call, _objectType.GetProperty(stringColumn.PropertyName).GetGetMethod());
                        il.Emit(OpCodes.Ldarg_S, (byte)5);
                        il.Emit(OpCodes.Callvirt, typeof(IStringColumn).GetMethod("SetString"));

                        sci++;
                        break;

                    case EFieldType.BitSet:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            il.Emit(OpCodes.Ret);

            return (Action<object, ByteArray, IFixedWidthColumn[], long, IStringColumn[], ITransactionContext>)
                method.CreateDelegate(
                    typeof(Action<object, ByteArray, IFixedWidthColumn[], long, IStringColumn[], ITransactionContext>));
        }


        /*
.method public hidebysig static object  GenerateItem(valuetype [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray bitset,
                                             class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn[] fixdCols,
                                             int64 rowid,
                                             class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn[] stringColumns,
                                             class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext readContext) cil managed
{
// Code size       112 (0x70)
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
IL_0027:  brtrue.s   IL_003a
IL_0029:  ldloc.0
IL_002a:  ldarg.3
IL_002b:  ldc.i4.0
IL_002c:  ldelem.ref
IL_002d:  ldarg.2
IL_002e:  ldarg.s    readContext
IL_0030:  callvirt   instance string [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::GetString(int64,
                                                                                                      class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext)
IL_0035:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::set_Mode(string)
IL_003a:  ldarga.s   bitset
IL_003c:  ldc.i4.2
IL_003d:  call       instance bool [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::IsSet(int32)
IL_0042:  brtrue.s   IL_0053
IL_0044:  ldloc.0
IL_0045:  ldarg.1
IL_0046:  ldc.i4.1
IL_0047:  ldelem.ref
IL_0048:  ldarg.2
IL_0049:  callvirt   instance int64 [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::GetInt64(int64)
IL_004e:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::set_Timestamp(int64)
IL_0053:  ldarga.s   bitset
IL_0055:  ldc.i4.3
IL_0056:  call       instance bool [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::IsSet(int32)
IL_005b:  brtrue.s   IL_006e
IL_005d:  ldloc.0
IL_005e:  ldarg.3
IL_005f:  ldc.i4.1
IL_0060:  ldelem.ref
IL_0061:  ldarg.2
IL_0062:  ldarg.s    readContext
IL_0064:  callvirt   instance string [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::GetString(int64,
                                                                                                      class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext)
IL_0069:  callvirt   instance void Apaf.NFSdb.Tests.Columns.ThriftModel.Quote::set_Sym(string)
IL_006e:  ldloc.0
IL_006f:  ret
} // end of method ExpressionTests::GenerateItem


* */

        private Func<ByteArray, IFixedWidthColumn[], long, IStringColumn[],
            IReadContext, object> GenerateFillMethod()
        {
            ConstructorInfo constructor = _objectType.GetConstructor(Type.EmptyTypes);
            MethodInfo isSet = typeof(ByteArray).GetMethod("IsSet");

            if (constructor == null)
                throw new NFSdbConfigurationException("No default contructor found on type " + _objectType);
            var argTypes = new[]
            {
                typeof (ByteArray), typeof (IFixedWidthColumn[]),
                typeof (long), typeof (IStringColumn[]), typeof (IReadContext)
            };
            var method = new DynamicMethod("ReadColumns8" + _objectType.Name + Guid.NewGuid().ToString().Substring(10),
                MethodAttributes.Static | MethodAttributes.Public, CallingConventions.Standard,
                _objectType, argTypes, _objectType, true);

            ILGenerator il = method.GetILGenerator();
            Label[] notSetLabels = _allDataColumns.Select(c => il.DefineLabel()).ToArray();
            il.DeclareLocal(_objectType);
            il.Emit(OpCodes.Newobj, constructor);
            il.Emit(OpCodes.Stloc_0);

            int fci = 0;
            int sci = 0;
            for (int i = 0; i < _allDataColumns.Length; i++)
            {
                var column = _allDataColumns[i];
                EFieldType fieldType = column.DataType;
                switch (fieldType)
                {
                    case EFieldType.Byte:
                    case EFieldType.Bool:
                    case EFieldType.Int16:
                    case EFieldType.Int32:
                    case EFieldType.Int64:
                    case EFieldType.Double:

                        var fixedCol = _fixedColumns[fci];
                        if (fixedCol != column)
                        {
                            throw new NFSdbInitializationException(
                                "Error generating Object Reader. Fixed column order does not match columns order");
                        }

                        il.Emit(OpCodes.Ldarga_S, (byte)0);
                        il.Emit(OpCodes.Ldc_I4, i);
                        il.Emit(OpCodes.Call, isSet);
                        il.Emit(OpCodes.Brtrue_S, notSetLabels[i]);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldarg_1);
                        il.Emit(OpCodes.Ldc_I4, fci);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_2);
                        il.Emit(OpCodes.Callvirt, GetGetMethod(fixedCol));
                        il.Emit(OpCodes.Callvirt, _objectType.GetProperty(fixedCol.PropertyName).GetSetMethod());

                        il.MarkLabel(notSetLabels[i]);

                        fci++;
                        break;

                    case EFieldType.String:
                    case EFieldType.Symbol:
                        var stringColumn = _stringColumns[sci];
                        if (stringColumn != column)
                        {
                            throw new NFSdbInitializationException(
                                "Error generating Object Reader. String column order does not match all columns order");
                        }

                        il.Emit(OpCodes.Ldarga_S, (byte)0);
                        il.Emit(OpCodes.Ldc_I4, i);
                        il.Emit(OpCodes.Call, isSet);
                        il.Emit(OpCodes.Brtrue_S, notSetLabels[i]);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Ldc_I4, sci);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_2);
                        il.Emit(OpCodes.Ldarg_S, (byte)4);
                        il.Emit(OpCodes.Callvirt, GetGetMethod(stringColumn));
                        il.Emit(OpCodes.Callvirt, _objectType.GetProperty(stringColumn.PropertyName).GetSetMethod());

                        il.MarkLabel(notSetLabels[i]);

                        sci++;
                        break;

                    case EFieldType.BitSet:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ret);

            return (Func<ByteArray, IFixedWidthColumn[], long, IStringColumn[], IReadContext, object>)
                method.CreateDelegate(
                    typeof(Func<ByteArray, IFixedWidthColumn[], long, IStringColumn[], IReadContext, object>));
        }


        private MethodInfo GetGetMethod(FieldData column)
        {
            if (column.DataType == EFieldType.String
                || column.DataType == EFieldType.Symbol)
            {
                return typeof(IStringColumn).GetMethod("GetString");
            }
            EFieldType fieldType = column.DataType;
            return typeof(IFixedWidthColumn).GetMethod("Get" + fieldType);
        }

        private FieldInfo GetIssetFieldInfo(Type issetType, string propertyName)
        {
            return issetType.GetField(GetFieldName(propertyName), BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        }

        private string GetFieldName(string propertyName)
        {
            return propertyName.Substring(0, 1).ToLower() + propertyName.Substring(1);
        }

    }
}