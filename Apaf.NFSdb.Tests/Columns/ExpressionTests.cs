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
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Storage.Serializer;
using Apaf.NFSdb.Core.Tx;
using Apaf.NFSdb.Tests.Columns.ThriftModel;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Columns
{
    [TestFixture]
    public class ExpressionTests
    {
        public class QuotePoco
        {
            public long Timestamp { get; set; }
            public string Sym { get; set; }
            public double? Bid { get; set; }
            public double? Ask { get; set; }
            public int BidSize { get; set; }
            public int AskSize { get; set; }
            public string Mode { get; set; }
            public string Ex { get; set; }
        }

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

        [Test]
        public void SetBilliionNullableDoubles()
        {
            double? val = null;
            var rand = new RandomDouble();
            var ss = new Stopwatch();
            ss.Start();
            for (int i = 0; i < (int) 2E9; i++)
            {
                val = rand.Next();
            }
            ss.Stop();
            Console.WriteLine(ss.Elapsed);
        }

        [Test]
        public void SetBilliionDoubles()
        {
            double val = 0.0;
            var rand = new RandomDouble();
            var ss = new Stopwatch();
            ss.Start();
            for (int i = 0; i < (int)2E9; i++)
            {
                val = rand.Next();
            }
            ss.Stop();
            Console.WriteLine(ss.Elapsed);
        }

        public class RandomDouble
        {
            private Random _r = new Random();
            public double Next()
            {
                return _r.NextDouble();
            }
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

        public static object ReadItemPoco(ByteArray bitset,
            IFixedWidthColumn[] fixedCols,
            FixedColumnNullableWrapper[] nullableCols,
            long rowid, IStringColumn[] stringColumns, IReadContext readContext)
        {
            var q = new QuotePoco();
            q.Timestamp = fixedCols[0].GetInt64(rowid);
            q.Sym = stringColumns[0].GetString(rowid, readContext);

            if (!bitset.IsSet(0))
            {
                q.Ask = fixedCols[1].GetDouble(rowid);
            }

            if (!bitset.IsSet(1))
            {
                q.Bid = fixedCols[2].GetDouble(rowid);
            }

            q.BidSize = fixedCols[2].GetInt32(rowid);
            q.AskSize = fixedCols[3].GetInt32(rowid);

            q.Mode = stringColumns[1].GetString(rowid, readContext);
            q.Ex = stringColumns[2].GetString(rowid, readContext);

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

        public static void WriteItemPoco(object obj,
            ByteArray bitset,
            IFixedWidthColumn[] fixedCols, 
            FixedColumnNullableWrapper[] nullableCols, long rowid,
            IStringColumn[] stringColumns, ITransactionContext readContext)
        {
            var q = (QuotePoco)obj;
            fixedCols[0].SetInt64(rowid, q.Timestamp, readContext);
            
            stringColumns[0].SetString(rowid, q.Sym, readContext);

            nullableCols[0].SetNullableDouble(rowid, q.Bid, bitset, readContext);
            nullableCols[1].SetNullableDouble(rowid, q.Ask, bitset, readContext);

            fixedCols[0].SetInt32(rowid, q.BidSize, readContext);
            fixedCols[1].SetInt32(rowid, q.AskSize, readContext);

            stringColumns[1].SetString(rowid, q.Mode, readContext);
            stringColumns[1].SetString(rowid, q.Ex, readContext);
        }

        public void Set(Quote q, double val)
        {
            q.Bid = val;
        }
    }
}