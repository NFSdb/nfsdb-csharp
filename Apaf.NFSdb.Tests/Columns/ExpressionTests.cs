using System;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using Apaf.NFSdb.Core;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Columns
{
    [TestFixture]
    public class ExpressionTests
    {
        private const int LOOP_NUM = (int) 1E3;

        [Test]
        public void SetterExperssionTest()
        {
            var arg = Expression.Parameter(typeof (Quote));
            var expr = Expression.Property(arg, "Bid");
            var param = Expression.Parameter(typeof (double));
            var assign = Expression.Assign(expr, param);
            Action<Quote, double> propSetter =
                Expression
                    .Lambda<Action<Quote, double>>(assign, arg, param)
                    .Compile();

            var quote = new Quote();
            var sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < LOOP_NUM; i ++)
            {
                propSetter(quote, 1.1 + i);
            }
            sw.Stop();
            Console.WriteLine(sw.Elapsed.ToString());
            Assert.That(quote.Bid, Is.EqualTo(LOOP_NUM + 0.1));
        }

        public class Variant
        {
            public string StringVal;
            public int Int32Val;
            public short Int16Val;
            public long Int64Val;
            public byte ByteVal;
            public double DoubleVal;
            public bool BoolVal;
        }

        public static void SetString(object q, Variant var)
        {
            ((Quote) q).Sym = var.StringVal;
        }

        
        private Action<Quote, string> GenMethodAssignment(string propName)
        {
            var setMethod = typeof(Quote).GetMethod("set_" + propName);
            if (setMethod == null)
                throw new InvalidOperationException("no property setter available");

            var argTypes = new[] { typeof(object), typeof(string) };
            var method = new DynamicMethod("__dynamicMethod_Set_" + propName, null, argTypes, this.GetType());
            
            var il = method.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Castclass, typeof(Quote));
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Call, setMethod);
            il.Emit(OpCodes.Ret);
            method.DefineParameter(1, ParameterAttributes.In, "instance");
            method.DefineParameter(2, ParameterAttributes.In, "value");

            var retval = (Action<Quote, string>)method.CreateDelegate(typeof(Action<Quote, string>));
            return retval;
        }

        [Test]
        public void SetterEmit()
        {
            var quote = new Quote();
            var setter = GenMethodAssignment("Ex");
            var sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < LOOP_NUM; i++)
            {
                setter(quote, "some string 1");
            }
            sw.Stop();
            Console.WriteLine(sw.Elapsed.ToString());
        }

        [Test]
        public void ReflectionEmit()
        {
            var quote = new Quote();
            var prop = typeof (Quote).GetProperty("Ex");
            Action<object, object> setter = (object o1, object v1) => { prop.SetValue(o1, v1, null); };
            var sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < LOOP_NUM; i++)
            {
                setter(quote, "some string 1");
            }
            sw.Stop();
            Console.WriteLine(sw.Elapsed.ToString());
        }

        [Test]
        public void SetterNativeTest()
        {
            object quote = new Quote();
            var sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < LOOP_NUM; i++)
            {
                ((Quote)quote).Bid = i + 1.1;
            }
            sw.Stop();
            Console.WriteLine(sw.Elapsed.ToString());
            Assert.That(((Quote)quote).Bid, Is.EqualTo(LOOP_NUM + 0.1));
        }

        [Test]
        public void GetIl()
        {
            var mi = GetType().GetMethod("Set");
            byte[] bd = mi.GetMethodBody().GetILAsByteArray();

        }

        public static object GenerateItem(ByteArray bitset, IFixedWidthColumn[] fixdCols,
            long rowid, IStringColumn[] stringColumns, IReadContext readContext)
        {
            var q = new Quote();
            if (!bitset.IsSet(0))
            {
                q.AskSize = fixdCols[0].GetInt32(rowid);
            }

            if (!bitset.IsSet(1))
            {
                q.Mode = stringColumns[0].GetString(rowid, readContext);
            }

            if (!bitset.IsSet(2))
            {
                q.Timestamp = fixdCols[1].GetInt64(rowid);
            }

            if (!bitset.IsSet(3))
            {
                q.Sym = stringColumns[1].GetString(rowid, readContext);
            }
            return q;
        }

        public static void WriteItem(object obj, 
            ByteArray bitset, 
            IFixedWidthColumn[] fixedCols, long rowid, 
            IStringColumn[] stringColumns, ITransactionContext readContext)
        {
            var q = (Quote) obj;
            bitset.Set(0, !q.__isset.timestamp);
            fixedCols[0].SetInt64(rowid, q.Timestamp, readContext);
         
            bitset.Set(1, !q.__isset.bid);
            fixedCols[1].SetDouble(rowid, q.Bid, readContext);

            bitset.Set(2, !q.__isset.bid);
            stringColumns[0].SetString(rowid, q.Mode, readContext);
        }

        public void Set(Quote q, double val)
        {
            q.Bid = val;
        }
    }
}