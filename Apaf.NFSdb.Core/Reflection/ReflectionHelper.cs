#region copyright
/*
 * Copyright (c) 2014. APAF (Alex Pelagenko).
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

namespace Apaf.NFSdb.Core.Reflection
{
    public static class ReflectionHelper
    {
        public static Func<object> CreateConstructorDelegate(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");

            var constructor = type.GetConstructor(Type.EmptyTypes);
            if (constructor == null) throw new NotSupportedException("Default constructor does not exists");

            var dynamic = new DynamicMethod(string.Empty,
                typeof (object),
                new Type[0],
                type);

            var il = dynamic.GetILGenerator();
            il.DeclareLocal(type);
            il.Emit(OpCodes.Newobj, constructor);
            il.Emit(OpCodes.Stloc_0);
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ret);

            return (Func<object>)dynamic.CreateDelegate(typeof(Func<object>));
        }
        
        public static Func<T, long> CreateTimestampDelegate<T>(string timestampField)
        {
            var type = typeof (T);
            var dynamic = new DynamicMethod("Test_timestamp" + Guid.NewGuid(),
                typeof(long), new[] { type },type);
            
            var il = dynamic.GetILGenerator();
            il.DeclareLocal(type);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, type.GetField("_" + timestampField, BindingFlags.Instance
                | BindingFlags.NonPublic));
            il.Emit(OpCodes.Ret);

            return (Func<T, long>)dynamic.CreateDelegate(typeof(Func<T, long>));
        }
    }
}