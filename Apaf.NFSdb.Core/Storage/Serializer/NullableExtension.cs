using System;
using System.Reflection;
using System.Reflection.Emit;

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class NullableExtension
    {
        /*
         .method public hidebysig instance void  SetValue(!T newValue) cil managed
        {
          // Code size       15 (0xf)
          .maxstack  8
          IL_0000:  ldarg.0
          IL_0001:  ldc.i4.1
          IL_0002:  stfld      bool class Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<!T>::hasValue
          IL_0007:  ldarg.0
          IL_0008:  ldarg.1
          IL_0009:  stfld      !0 class Apaf.NFSdb.Tests.Columns.ExpressionTests/MyNullable`1<!T>::'value'
          IL_000e:  ret
        } // end of method MyNullable`1::SetValue
         */

        public static MethodInfo GetSetValue<T>() where T : struct
        {
            var genericType = typeof(T?);
            var method = new DynamicMethod("SetValue",
                MethodAttributes.Public | MethodAttributes.Static, CallingConventions.Standard,
                null, new[] { typeof(T) }, genericType, true);

            var allFields = genericType.GetFields(BindingFlags.NonPublic | BindingFlags.Instance);
            var hasValueField = genericType.GetField("hasValue", BindingFlags.NonPublic | BindingFlags.Instance);
            var valueField = genericType.GetField("value", BindingFlags.NonPublic | BindingFlags.Instance);
            if (hasValueField == null || valueField == null)
            {
                throw new NotSupportedException("The platform Nullable implementation " +
                                                "is different from expected");
            }

            var il = method.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Stfld, hasValueField);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Stfld, valueField);
            il.Emit(OpCodes.Ret);

            return method;
        }
    }
}