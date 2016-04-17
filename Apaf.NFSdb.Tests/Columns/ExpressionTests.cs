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
using System.Runtime.Serialization;
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
        public class QuotePoco
        {
            private long _timestamp;
            private string _sym;
            private MyNullable<double> _bid;
            private MyNullable<double> _ask;
            private int _bidSize;
            private int _askSize;
            private string _mode;
            private string _ex;

            public long Timestamp
            {
                get { return _timestamp; }
                set { _timestamp = value; }
            }

            public string Sym
            {
                get { return _sym; }
                set { _sym = value; }
            }

            public MyNullable<double> Bid
            {
                get { return _bid; }
                set { _bid = value; }
            }

            public MyNullable<double> Ask
            {
                get { return _ask; }
                set { _ask = value; }
            }

            public int BidSize
            {
                get { return _bidSize; }
                set { _bidSize = value; }
            }

            public int AskSize
            {
                get { return _askSize; }
                set { _askSize = value; }
            }

            public string Mode
            {
                get { return _mode; }
                set { _mode = value; }
            }

            public string Ex
            {
                get { return _ex; }
                set { _ex = value; }
            }

            public static object ReadItemPoco(ByteArray bitset,
                IFixedWidthColumn[] fixedCols,
                long rowid, IRefTypeColumn[] stringColumns, ReadContext readContext)
            {
                var q = (QuotePoco)FormatterServices.GetUninitializedObject(typeof(QuotePoco));
                q._timestamp = fixedCols[0].GetInt64(rowid);

                if (!bitset.IsSet(0))
                {
                    q._sym = (string) stringColumns[0].GetValue(rowid, readContext);
                }

                if (!bitset.IsSet(1))
                {
                    q._ask.value = fixedCols[0].GetDouble(rowid);
                    q._ask.hasValue = true;
                }

                if (!bitset.IsSet(2))
                {
                    q._bid.value = fixedCols[1].GetDouble(rowid);
                    q._bid.hasValue = true;
                }

                q._bidSize = fixedCols[2].GetInt32(rowid);
                q._askSize = fixedCols[3].GetInt32(rowid);

                if (!bitset.IsSet(3))
                {
                    q._mode = (string) stringColumns[1].GetValue(rowid, readContext);
                }
                if (!bitset.IsSet(4))
                {
                    q._ex = (string) stringColumns[2].GetValue(rowid, readContext);
                }
                return q;
            }

            public static void WriteItemPoco(
                object obj,
                ByteArray bitset,
                IFixedWidthColumn[] fixedCols,
                long rowid, 
                IRefTypeColumn[] stringColumns, 
                ITransactionContext readContext)
            {
                var q = (QuotePoco)obj;
                fixedCols[0].SetInt64(rowid, q._timestamp);

                var pd = readContext.GetPartitionTx();
                bitset.Set(0, q._sym == null);
                stringColumns[0].SetValue(rowid, q._sym, pd);

                bool isnull = !q._ask.hasValue;
                bitset.Set(1, isnull);
                if (!isnull)
                {
                    fixedCols[0].SetDouble(rowid, q._ask.value);
                }

                isnull = !q._bid.hasValue;
                bitset.Set(2, isnull);
                if (!isnull)
                {
                    fixedCols[1].SetDouble(rowid, q._bid.value);
                }

                fixedCols[2].SetInt32(rowid, q._bidSize);
                fixedCols[3].SetInt32(rowid, q._askSize);

                bitset.Set(2, q._mode == null);
                stringColumns[1].SetValue(rowid, q._mode, pd);

                bitset.Set(3, q._ex == null);
                stringColumns[1].SetValue(rowid, q._ex, pd);
            }
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
        public void TypedRefTest()
        {
            var n = new MyNullable<double>();
            MyNullable<double>.SetValue(__makeref(n), 4.0);

            Assert.That(n.value, Is.EqualTo(4.0));
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
            long rowid, IRefTypeColumn[] stringColumns, ReadContext readContext)
        {
            var q = new Quote();
            if (!bitset.IsSet(0))
            {
                q.AskSize = fixdCols[0].GetInt32(rowid);
            }

            if (!bitset.IsSet(1))
            {
                q.Mode = (string) stringColumns[0].GetValue(rowid, readContext);
            }

            if (!bitset.IsSet(2))
            {
                q.Timestamp = fixdCols[1].GetInt64(rowid);
            }

            if (!bitset.IsSet(3))
            {
                q.Sym = (string) stringColumns[1].GetValue(rowid, readContext);
            }
            return q;
        }

        public static void WriteItem(object obj, 
            ByteArray bitset, 
            IFixedWidthColumn[] fixedCols, long rowid, 
            IRefTypeColumn[] stringColumns, ITransactionContext readContext)
        {
            var q = (Quote) obj;
            bitset.Set(0, !q.__isset.timestamp);
            fixedCols[0].SetInt64(rowid, q.Timestamp);
         
            bitset.Set(1, !q.__isset.bid);
            fixedCols[1].SetDouble(rowid, q.Bid);

            bitset.Set(2, !q.__isset.bid);
            stringColumns[0].SetValue(rowid, q.Mode, readContext.GetPartitionTx());
        }

        public void Set(Quote q, double val)
        {
            q.Bid = val;
        }

        public struct MyNullable<T> where T : struct
        {
            public bool hasValue;
            public T value;

            public void SetNull()
            {
                hasValue = false;
                value = default(T);
            }

            public static void SetValue(TypedReference reference, T newValue)
            {
                __refvalue(reference, MyNullable<T>).value = newValue;
                __refvalue(reference, MyNullable<T>).hasValue = true;
            }
        }
    }
}