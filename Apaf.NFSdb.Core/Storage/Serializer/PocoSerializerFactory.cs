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
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
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
        private Func<ByteArray, IFixedWidthColumn[], long, IStringColumn[], IReadContext, object> _readMethod;
        private Action<object, ByteArray, IFixedWidthColumn[], long, IStringColumn[], ITransactionContext> _writeMethod;
        private static readonly Guid GENERATOR_MARK_GUID = Guid.NewGuid();
        private bool _isAnonymouse;
        private Dictionary<string, string> _propertyToFields;

        public void Initialize(Type objectType)
        {
            _objectType = objectType;
            _isAnonymouse = CheckIfAnonymousType(objectType);
            _allColumns = ParseColumnsImpl();
            _allDataColumns = _allColumns
                .Where(c => c.DataType != EFieldType.BitSet)
                .ToArray();

            _readMethod = GenerateReadMethod();
            _writeMethod = GenerateWriteMethod();
        }

        public IList<FieldData> ParseColumns()
        {
            return _allColumns;
        }

        public IFieldSerializer CreateFieldSerializer(IEnumerable<IColumn> columns)
        {
            return new PocoObjectSerializer(columns, _readMethod, _writeMethod);
        }

        /*
.method public hidebysig static object  ReadItemPoco(valuetype [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray bitset,
                                                     class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn[] fixedCols,
                                                     int64 rowid,
                                                     class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn[] stringColumns,
                                                     class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext readContext) cil managed
{
  // Code size       233 (0xe9)
  .maxstack  4
  .locals init ([0] class Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco q)
  IL_0000:  ldtoken    Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco
  IL_0005:  call       class [mscorlib]System.Type [mscorlib]System.Type::GetTypeFromHandle(valuetype [mscorlib]System.RuntimeTypeHandle)
  IL_000a:  call       object [mscorlib]System.Runtime.Serialization.FormatterServices::GetUninitializedObject(class [mscorlib]System.Type)
  IL_000f:  castclass  Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco
  IL_0014:  stloc.0
  IL_0015:  ldloc.0
  IL_0016:  ldarg.1
  IL_0017:  ldc.i4.0
  IL_0018:  ldelem.ref
  IL_0019:  ldarg.2
  IL_001a:  callvirt   instance int64 [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::GetInt64(int64)
  IL_001f:  stfld      int64 Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_timestamp
  IL_0024:  ldarga.s   bitset
  IL_0026:  ldc.i4.0
  IL_0027:  call       instance bool [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::IsSet(int32)
  IL_002c:  brtrue.s   IL_003f
  IL_002e:  ldloc.0
  IL_002f:  ldarg.3
  IL_0030:  ldc.i4.0
  IL_0031:  ldelem.ref
  IL_0032:  ldarg.2
  IL_0033:  ldarg.s    readContext
  IL_0035:  callvirt   instance string [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::GetString(int64,
                                                                                                        class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext)
  IL_003a:  stfld      string Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_sym
  IL_003f:  ldarga.s   bitset
  IL_0041:  ldc.i4.1
  IL_0042:  call       instance bool [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::IsSet(int32)
  IL_0047:  brtrue.s   IL_0069
  IL_0049:  ldloc.0
  IL_004a:  ldflda     valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64> Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_ask
  IL_004f:  ldarg.1
  IL_0050:  ldc.i4.0
  IL_0051:  ldelem.ref
  IL_0052:  ldarg.2
  IL_0053:  callvirt   instance float64 [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::GetDouble(int64)
  IL_0058:  stfld      !0 valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64>::'value'
  IL_005d:  ldloc.0
  IL_005e:  ldflda     valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64> Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_ask
  IL_0063:  ldc.i4.1
  IL_0064:  stfld      bool valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64>::hasValue
  IL_0069:  ldarga.s   bitset
  IL_006b:  ldc.i4.2
  IL_006c:  call       instance bool [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::IsSet(int32)
  IL_0071:  brtrue.s   IL_0093
  IL_0073:  ldloc.0
  IL_0074:  ldflda     valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64> Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_bid
  IL_0079:  ldarg.1
  IL_007a:  ldc.i4.1
  IL_007b:  ldelem.ref
  IL_007c:  ldarg.2
  IL_007d:  callvirt   instance float64 [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::GetDouble(int64)
  IL_0082:  stfld      !0 valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64>::'value'
  IL_0087:  ldloc.0
  IL_0088:  ldflda     valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64> Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_bid
  IL_008d:  ldc.i4.1
  IL_008e:  stfld      bool valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64>::hasValue
  IL_0093:  ldloc.0
  IL_0094:  ldarg.1
  IL_0095:  ldc.i4.2
  IL_0096:  ldelem.ref
  IL_0097:  ldarg.2
  IL_0098:  callvirt   instance int32 [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::GetInt32(int64)
  IL_009d:  stfld      int32 Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_bidSize
  IL_00a2:  ldloc.0
  IL_00a3:  ldarg.1
  IL_00a4:  ldc.i4.3
  IL_00a5:  ldelem.ref
  IL_00a6:  ldarg.2
  IL_00a7:  callvirt   instance int32 [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::GetInt32(int64)
  IL_00ac:  stfld      int32 Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_askSize
  IL_00b1:  ldarga.s   bitset
  IL_00b3:  ldc.i4.3
  IL_00b4:  call       instance bool [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::IsSet(int32)
  IL_00b9:  brtrue.s   IL_00cc
  IL_00bb:  ldloc.0
  IL_00bc:  ldarg.3
  IL_00bd:  ldc.i4.1
  IL_00be:  ldelem.ref
  IL_00bf:  ldarg.2
  IL_00c0:  ldarg.s    readContext
  IL_00c2:  callvirt   instance string [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::GetString(int64,
                                                                                                        class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext)
  IL_00c7:  stfld      string Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_mode
  IL_00cc:  ldarga.s   bitset
  IL_00ce:  ldc.i4.4
  IL_00cf:  call       instance bool [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::IsSet(int32)
  IL_00d4:  brtrue.s   IL_00e7
  IL_00d6:  ldloc.0
  IL_00d7:  ldarg.3
  IL_00d8:  ldc.i4.2
  IL_00d9:  ldelem.ref
  IL_00da:  ldarg.2
  IL_00db:  ldarg.s    readContext
  IL_00dd:  callvirt   instance string [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::GetString(int64,
                                                                                                        class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Storage.IReadContext)
  IL_00e2:  stfld      string Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_ex
  IL_00e7:  ldloc.0
  IL_00e8:  ret
} // end of method QuotePoco::ReadItemPoco

         * */

        private Func<ByteArray, IFixedWidthColumn[], long, IStringColumn[], IReadContext, object> GenerateReadMethod()
        {
            ConstructorInfo constructor = _objectType.GetConstructor(Type.EmptyTypes);
            var isSetMethod = typeof (ByteArray).GetMethod("IsSet");
            var argTypes = new[]
            {
                typeof (ByteArray), typeof (IFixedWidthColumn[]), 
                typeof (long), typeof (IStringColumn[]), typeof (IReadContext)
            };

            var method = new DynamicMethod("Rd" + _objectType.GUID + GENERATOR_MARK_GUID,
                MethodAttributes.Static | MethodAttributes.Public, CallingConventions.Standard,
                _objectType, argTypes, _objectType, true);

            ILGenerator il = method.GetILGenerator();
            il.DeclareLocal(_objectType);
            if (constructor == null)
            {
                il.Emit(OpCodes.Ldtoken, _objectType);
                il.Emit(OpCodes.Call, typeof (Type).GetMethod("GetTypeFromHandle"));
                il.Emit(OpCodes.Call, typeof (FormatterServices).GetMethod("GetUninitializedObject"));
                il.Emit(OpCodes.Castclass, _objectType);
                il.Emit(OpCodes.Stloc_0);
            }
            else
            {
                il.Emit(OpCodes.Newobj, constructor);
                il.Emit(OpCodes.Stloc_0);
            }
            Label[] notSetLabels = _allDataColumns.Select(c => il.DefineLabel()).ToArray();

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
                    if (isString)
                    {
                        // Use IStringColumn[].
                        il.Emit(OpCodes.Ldarga_S, (byte)0);
                        il.Emit(OpCodes.Ldc_I4, nulli++);
                        il.Emit(OpCodes.Call, isSetMethod);
                        il.Emit(OpCodes.Brtrue_S, notSetLabels[i]);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Ldc_I4, sci++);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_2);
                        il.Emit(OpCodes.Ldarg_S, 4);
                        il.Emit(OpCodes.Callvirt, column.GetGetMethod());
                        il.Emit(OpCodes.Stfld, GetFieldInfo(column.PropertyName));
                        il.MarkLabel(notSetLabels[i]);
                    }
                    else if (!column.Nulllable)
                    {
                        // Use IFixedWidthColumn[].
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldarg_1);
                        il.Emit(OpCodes.Ldc_I4, fci++);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_2);
                        il.Emit(OpCodes.Callvirt, column.GetGetMethod());
                        il.Emit(OpCodes.Stfld, GetFieldInfo(column.PropertyName));
                    }
                    else
                    {
                        // Use FixedColumnNullableWrapper[].
                        il.Emit(OpCodes.Ldarga_S, (byte)0);
                        il.Emit(OpCodes.Ldc_I4, nulli);
                        il.Emit(OpCodes.Call, isSetMethod);
                        il.Emit(OpCodes.Brtrue_S, notSetLabels[i]);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldflda, GetFieldInfo(column.PropertyName));
                        il.Emit(OpCodes.Ldarg_1);
                        il.Emit(OpCodes.Ldc_I4, fci);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_2);
                        il.Emit(OpCodes.Callvirt, column.GetGetMethod());
                        il.Emit(OpCodes.Stfld, GetNullableValueField(column));
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldflda, GetFieldInfo(column.PropertyName));
                        il.Emit(OpCodes.Ldc_I4_1);
                        il.Emit(OpCodes.Stfld, GetNullableHasValueField(column));
                        il.MarkLabel(notSetLabels[i]);
                        nulli++;
                        fci++;
                    }
                }
            }
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ret);

            return (Func<ByteArray, IFixedWidthColumn[], 
                long, IStringColumn[], IReadContext, object>)
                method.CreateDelegate(
                    typeof(Func<ByteArray, IFixedWidthColumn[], 
                    long, IStringColumn[], IReadContext, object>));
        }

        private FieldInfo GetNullableHasValueField(FieldData column)
        {
            var ntype = GetNullableType(column);
            return ntype.GetField("hasValue", BindingFlags.Instance | BindingFlags.NonPublic);
        }

        private FieldInfo GetNullableValueField(FieldData column)
        {
            var ntype = GetNullableType(column);
            return ntype.GetField("value", BindingFlags.Instance | BindingFlags.NonPublic);
        }

        private static Type GetNullableType(FieldData column)
        {
            Type ntype;
            switch (column.DataType)
            {
                case EFieldType.Byte:
                    ntype = typeof (byte?);
                    break;
                case EFieldType.Bool:
                    ntype = typeof (bool?);
                    break;
                case EFieldType.Int16:
                    ntype = typeof (short?);
                    break;
                case EFieldType.Int32:
                    ntype = typeof (int?);
                    break;
                case EFieldType.Int64:
                    ntype = typeof (long?);
                    break;
                case EFieldType.Double:
                    ntype = typeof (double?);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            return ntype;
        }

        private FieldInfo GetFieldInfo(string propertyName)
        {
            var fieldName = _propertyToFields[propertyName];
            return _objectType.GetField(fieldName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        }

        /*
.method public hidebysig static void  WriteItemPoco(object obj,
                                                    valuetype [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray bitset,
                                                    class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn[] fixedCols,
                                                    int64 rowid,
                                                    class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn[] stringColumns,
                                                    class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext readContext) cil managed
{
  // Code size       262 (0x106)
  .maxstack  4
  .locals init ([0] class Apaf.NFSdb.Tests.Columns.Expressio
         * nTests/QuotePoco q,
           [1] bool isnull)
  IL_0000:  ldarg.0
  IL_0001:  castclass  Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco
  IL_0006:  stloc.0
  IL_0007:  ldarg.2
  IL_0008:  ldc.i4.0
  IL_0009:  ldelem.ref
  IL_000a:  ldarg.3
  IL_000b:  ldloc.0
  IL_000c:  ldfld      int64 Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_timestamp
  IL_0011:  ldarg.s    readContext
  IL_0013:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::SetInt64(int64,
                                                                                                         int64,
                                                                                                         class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_0018:  ldarga.s   bitset
  IL_001a:  ldc.i4.0
  IL_001b:  ldloc.0
  IL_001c:  callvirt   instance string Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::get_Sym()
  IL_0021:  ldnull
  IL_0022:  ceq
  IL_0024:  call       instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::Set(int32,
                                                                                            bool)
  IL_0029:  ldarg.s    stringColumns
  IL_002b:  ldc.i4.0
  IL_002c:  ldelem.ref
  IL_002d:  ldarg.3
  IL_002e:  ldloc.0
  IL_002f:  ldfld      string Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_sym
  IL_0034:  ldarg.s    readContext
  IL_0036:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::SetString(int64,
                                                                                                      string,
                                                                                                      class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_003b:  ldloc.0
  IL_003c:  ldflda     valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64> Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_ask
  IL_0041:  ldfld      bool valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64>::hasValue
  IL_0046:  ldc.i4.0
  IL_0047:  ceq
  IL_0049:  stloc.1
  IL_004a:  ldarga.s   bitset
  IL_004c:  ldc.i4.1
  IL_004d:  ldloc.1
  IL_004e:  call       instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::Set(int32,
                                                                                            bool)
  IL_0053:  ldloc.1
  IL_0054:  brtrue.s   IL_006c
  IL_0056:  ldarg.2
  IL_0057:  ldc.i4.0
  IL_0058:  ldelem.ref
  IL_0059:  ldarg.3
  IL_005a:  ldloc.0
  IL_005b:  ldflda     valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64> Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_ask
  IL_0060:  ldfld      !0 valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64>::'value'
  IL_0065:  ldarg.s    readContext
  IL_0067:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::SetDouble(int64,
                                                                                                          float64,
                                                                                                          class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_006c:  ldloc.0
  IL_006d:  ldflda     valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64> Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_bid
  IL_0072:  ldfld      bool valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64>::hasValue
  IL_0077:  ldc.i4.0
  IL_0078:  ceq
  IL_007a:  stloc.1
  IL_007b:  ldarga.s   bitset
  IL_007d:  ldc.i4.2
  IL_007e:  ldloc.1
  IL_007f:  call       instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::Set(int32,
                                                                                            bool)
  IL_0084:  ldloc.1
  IL_0085:  brtrue.s   IL_009d
  IL_0087:  ldarg.2
  IL_0088:  ldc.i4.1
  IL_0089:  ldelem.ref
  IL_008a:  ldarg.3
  IL_008b:  ldloc.0
  IL_008c:  ldflda     valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64> Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_bid
  IL_0091:  ldfld      !0 valuetype Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<float64>::'value'
  IL_0096:  ldarg.s    readContext
  IL_0098:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::SetDouble(int64,
                                                                                                          float64,
                                                                                                          class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_009d:  ldarg.2
  IL_009e:  ldc.i4.2
  IL_009f:  ldelem.ref
  IL_00a0:  ldarg.3
  IL_00a1:  ldloc.0
  IL_00a2:  ldfld      int32 Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_bidSize
  IL_00a7:  ldarg.s    readContext
  IL_00a9:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::SetInt32(int64,
                                                                                                         int32,
                                                                                                         class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_00ae:  ldarg.2
  IL_00af:  ldc.i4.3
  IL_00b0:  ldelem.ref
  IL_00b1:  ldarg.3
  IL_00b2:  ldloc.0
  IL_00b3:  ldfld      int32 Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_askSize
  IL_00b8:  ldarg.s    readContext
  IL_00ba:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IFixedWidthColumn::SetInt32(int64,
                                                                                                         int32,
                                                                                                         class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_00bf:  ldarga.s   bitset
  IL_00c1:  ldc.i4.2
  IL_00c2:  ldloc.0
  IL_00c3:  ldfld      string Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_mode
  IL_00c8:  ldnull
  IL_00c9:  ceq
  IL_00cb:  call       instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::Set(int32,
                                                                                            bool)
  IL_00d0:  ldarg.s    stringColumns
  IL_00d2:  ldc.i4.1
  IL_00d3:  ldelem.ref
  IL_00d4:  ldarg.3
  IL_00d5:  ldloc.0
  IL_00d6:  ldfld      string Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_mode
  IL_00db:  ldarg.s    readContext
  IL_00dd:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::SetString(int64,
                                                                                                      string,
                                                                                                      class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_00e2:  ldarga.s   bitset
  IL_00e4:  ldc.i4.3
  IL_00e5:  ldloc.0
  IL_00e6:  ldfld      string Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_ex
  IL_00eb:  ldnull
  IL_00ec:  ceq
  IL_00ee:  call       instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.ByteArray::Set(int32,
                                                                                            bool)
  IL_00f3:  ldarg.s    stringColumns
  IL_00f5:  ldc.i4.1
  IL_00f6:  ldelem.ref
  IL_00f7:  ldarg.3
  IL_00f8:  ldloc.0
  IL_00f9:  ldfld      string Apaf.NFSdb.Tests.Columns.ExpressionTests/QuotePoco::_ex
  IL_00fe:  ldarg.s    readContext
  IL_0100:  callvirt   instance void [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Column.IStringColumn::SetString(int64,
                                                                                                      string,
                                                                                                      class [Apaf.NFSdb.Core]Apaf.NFSdb.Core.Tx.ITransactionContext)
  IL_0105:  ret
} // end of method QuotePoco::WriteItemPoco

         * */
        private Action<object, ByteArray, IFixedWidthColumn[], 
            long, IStringColumn[], ITransactionContext> 
            GenerateWriteMethod()
        {
            var argTypes = new[]
            {
                typeof (object), typeof (ByteArray), typeof (IFixedWidthColumn[]), 
                typeof (long), typeof (IStringColumn[]), typeof (ITransactionContext)
            };
            var setMethod = typeof(ByteArray).GetMethod("Set");
            var method = new DynamicMethod("Wt" + _objectType.GUID + GENERATOR_MARK_GUID,
                MethodAttributes.Static | MethodAttributes.Public, CallingConventions.Standard,
                null, argTypes, _objectType, true);

            ILGenerator il = method.GetILGenerator();
            il.DeclareLocal(_objectType);
            il.DeclareLocal(typeof(bool));
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Castclass, _objectType);
            il.Emit(OpCodes.Stloc_0);
            int fci = 0;
            int sci = 0;
            int nci = 0;
            Label[] notSetLabels = _allDataColumns.Select(c => il.DefineLabel()).ToArray();

            for (int i = 0; i < _allDataColumns.Length; i++)
            {
                var column = _allColumns[i];
                if (column.DataType != EFieldType.BitSet)
                {
                    if (!column.Nulllable)
                    {
                        il.Emit(OpCodes.Ldarg_2);
                        il.Emit(OpCodes.Ldc_I4, fci++);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldfld, GetFieldInfo(column.PropertyName));
                        il.Emit(OpCodes.Ldarg_S, (byte) 5);
                        il.Emit(OpCodes.Callvirt, column.GetSetMethod());
                    }
                    else if (column.DataType == EFieldType.String || column.DataType == EFieldType.Symbol)
                    {
                        il.Emit(OpCodes.Ldarga_S, 1);
                        il.Emit(OpCodes.Ldc_I4, nci++);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldfld, GetFieldInfo(column.PropertyName));
                        il.Emit(OpCodes.Ldnull);
                        il.Emit(OpCodes.Ceq);
                        il.Emit(OpCodes.Call, setMethod);
                        il.Emit(OpCodes.Ldarg_S, (byte)4);
                        il.Emit(OpCodes.Ldc_I4, sci++);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldfld, GetFieldInfo(column.PropertyName));
                        il.Emit(OpCodes.Ldarg_S, (byte)5);
                        il.Emit(OpCodes.Callvirt, column.GetSetMethod());
                    }
                    else
                    {
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldflda, GetFieldInfo(column.PropertyName));
                        il.Emit(OpCodes.Ldfld, GetNullableHasValueField(column));
                        il.Emit(OpCodes.Ldc_I4_0);
                        il.Emit(OpCodes.Ceq);
                        il.Emit(OpCodes.Stloc_1);
                        il.Emit(OpCodes.Ldarga_S, 1);
                        il.Emit(OpCodes.Ldc_I4, nci++);
                        il.Emit(OpCodes.Ldloc_1);
                        il.Emit(OpCodes.Call, setMethod);
                        il.Emit(OpCodes.Ldloc_1);
                        il.Emit(OpCodes.Brtrue_S, notSetLabels[i]);
                        il.Emit(OpCodes.Ldarg_2);
                        il.Emit(OpCodes.Ldc_I4, fci++);
                        il.Emit(OpCodes.Ldelem_Ref);
                        il.Emit(OpCodes.Ldarg_3);
                        il.Emit(OpCodes.Ldloc_0);
                        il.Emit(OpCodes.Ldflda, GetFieldInfo(column.PropertyName));
                        il.Emit(OpCodes.Ldfld, GetNullableValueField(column));
                        il.Emit(OpCodes.Ldarg_S, 5);
                        il.Emit(OpCodes.Callvirt, column.GetSetMethod());
                        il.MarkLabel(notSetLabels[i]);
                    }
                }
            }
            il.Emit(OpCodes.Ret);

            return (Action<object, ByteArray, IFixedWidthColumn[],
                long, IStringColumn[], ITransactionContext>)
                method.CreateDelegate(
                    typeof(Action<object, ByteArray, IFixedWidthColumn[], 
                    long, IStringColumn[], ITransactionContext>));

        }

        private IList<FieldData> ParseColumnsImpl()
        {
            _propertyToFields = new Dictionary<string, string>();

            // Properties.
            // Public.
            var fields =
                _objectType.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

            // Build.
            var cols = new List<FieldData>();
            int nullableCount = 0;

            foreach (FieldInfo field in fields)
            {
                var fieldName = field.Name;
                var propertyName = GetPropertyName(fieldName);
                _propertyToFields.Add(propertyName, fieldName);

                // Type.
                var propertyType = field.FieldType;
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
                    cols.Add(new FieldData(EFieldType.String, propertyName, true));
                    nullableCount++;
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

        public string GetPropertyName(string fieldName)
        {
            if (_isAnonymouse)
            {
                const string suffix = ">i__Field";
                const string prefix = "<";
                var name = RemovePrefixAndSuffix(fieldName, suffix, prefix);
                if (name != null) return name;
            }
            else
            {
                const string suffix = ">k__BackingField";
                const string prefix = "<";
                var name = RemovePrefixAndSuffix(fieldName, suffix, prefix);
                if (name != null) return name;
            }
            return GetQualifiedName(fieldName);
        }

        private static string RemovePrefixAndSuffix(string fieldName, string suffix, string prefix)
        {
            int removeLen = suffix.Length + prefix.Length;
            if (fieldName.Length > removeLen
                && fieldName.StartsWith(prefix) && fieldName.EndsWith(suffix))
            {
                {
                    return fieldName.Substring(prefix.Length, fieldName.Length - removeLen);
                }
            }
            return null;
        }

        private string GetQualifiedName(string fieldName)
        {
            return new string(fieldName.Where(Char.IsLetterOrDigit)
                .Select((c,i) => i == 0 ? Char.ToUpper(c) : c).ToArray());
        }

        private static bool CheckIfAnonymousType(Type type)
        {
            if (type == null)
                throw new ArgumentNullException("type");

            return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
                && type.IsGenericType && type.Name.Contains("AnonymousType")
                && (type.Name.StartsWith("<>") || type.Name.StartsWith("VB$"))
                && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
        }
    }
}