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