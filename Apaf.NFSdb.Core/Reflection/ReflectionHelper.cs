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
using System.Reflection;
using System.Reflection.Emit;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core.Reflection
{
    public static class ReflectionHelper
    {
        public static Func<object> CreateConstructorDelegate(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");

            var constructor = type.GetConstructor(Type.EmptyTypes);
            if (constructor == null) throw new NotSupportedException("Default constructor does not exists");

            var dynamic = new DynamicMethod("Create_item_nfsdb",
                typeof (object),
                new Type[0],
                type, 
                true);

            var il = dynamic.GetILGenerator();
            il.DeclareLocal(type);
            il.Emit(OpCodes.Newobj, constructor);
            il.Emit(OpCodes.Stloc_0);
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ret);

            return (Func<object>)dynamic.CreateDelegate(typeof(Func<object>));
        }

        public static Func<T, TT> CreateFieldsAccessDelegate<T, TT>(string fieldName)
        {
            var type = typeof(T);
            var field = type.GetField(fieldName, BindingFlags.Instance
                                                      | BindingFlags.NonPublic);
            if (field == null)
            {
                throw new NFSdbConfigurationException(
                    string.Format("Field {0} cannot be found", fieldName));
            }
            if (field.FieldType != typeof (TT))
            {
                throw new NFSdbConfigurationException(
                    string.Format("Field {0} cannot is of type {1} while expcted to be {2}", fieldName,
                    field.FieldType, typeof(TT)));
            }

            var dynamic = new DynamicMethod("Get_" + fieldName + "_nfsdb" + Guid.NewGuid(),
                typeof(TT), new[] { type }, type, true);

            var il = dynamic.GetILGenerator();
            il.DeclareLocal(type);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, field);
            il.Emit(OpCodes.Ret);

            return (Func<T, TT>)dynamic.CreateDelegate(typeof(Func<T, TT>));
        }

        public static Func<object, DateTime> CreateTimestampDelegate<T>(string timestampField)
        {
            var type = typeof (T);
            var field = type.GetField(timestampField, BindingFlags.Instance
                                                      | BindingFlags.NonPublic);
            if (field == null)
            {
                throw new NFSdbConfigurationException(
                    string.Format("Timestamp field {0} cannot be found", timestampField));
            }

            var dynamic = new DynamicMethod("Get_timestamp_nfsdb" + Guid.NewGuid(),
                typeof (DateTime), new[] {typeof(object)}, type, true);

            var il = dynamic.GetILGenerator();
            il.DeclareLocal(type);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, field);

            if (field.FieldType == typeof (long))
            {
                il.Emit(OpCodes.Call, typeof(DateUtils).GetMethod("UnixTimestampToDateTime"));
            }
            il.Emit(OpCodes.Ret);

            return (Func<object, DateTime>)dynamic.CreateDelegate(typeof(Func<object, DateTime>));
        }
    }
}