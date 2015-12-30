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
using System.Collections.Concurrent;
using System.Threading;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Exceptions;

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class JournalColumnRegistry
    {
        private static readonly Lazy<JournalColumnRegistry> INSTANCE = new Lazy<JournalColumnRegistry>(CreateSigleton,
            LazyThreadSafetyMode.PublicationOnly);

        private readonly ConcurrentDictionary<EFieldType, IColumnDataType> _byFieldType =
            new ConcurrentDictionary<EFieldType, IColumnDataType>();

        private readonly ConcurrentDictionary<Type, IColumnDataType> _byDataType =
            new ConcurrentDictionary<Type, IColumnDataType>();

        private static readonly FixedColumnDataType BYTE = new FixedColumnDataType(EFieldType.Byte, typeof(Byte));
        private static readonly FixedColumnDataType BOOL = new FixedColumnDataType(EFieldType.Bool, typeof(Boolean));
        private static readonly FixedColumnDataType INT16 = new FixedColumnDataType(EFieldType.Int16, typeof(Int16));
        private static readonly FixedColumnDataType INT32 = new FixedColumnDataType(EFieldType.Int32, typeof(Int32));
        private static readonly FixedColumnDataType INT64 = new FixedColumnDataType(EFieldType.Int64, typeof(Int64));
        private static readonly FixedColumnDataType DOUBLE = new FixedColumnDataType(EFieldType.Double, typeof(Double));
        private static readonly FixedColumnDataType STRING = new FixedColumnDataType(EFieldType.String, typeof(String));
        private static readonly FixedColumnDataType BITSET = new FixedColumnDataType(EFieldType.BitSet, typeof(ByteArray));
        private static readonly FixedColumnDataType SYMBOL = new FixedColumnDataType(EFieldType.Symbol, typeof(String));
        private static readonly FixedColumnDataType BINARY = new FixedColumnDataType(EFieldType.Binary, typeof(byte[]));
        private static readonly FixedColumnDataType DATE_TIME = new FixedColumnDataType(EFieldType.DateTime, typeof(DateTime));
        private static readonly FixedColumnDataType DATE_TIME_EPOCH_MS = new FixedColumnDataType(EFieldType.DateTimeEpochMs, typeof(DateTime));

        private static JournalColumnRegistry CreateSigleton()
        {
            var instance = new JournalColumnRegistry();
            instance.Add(BYTE);
            instance.Add(BOOL);
            instance.Add(INT16);
            instance.Add(INT32);
            instance.Add(INT64);
            instance.Add(DOUBLE);
            instance.Add(STRING);
            instance.Add(BITSET);
            instance.Add(SYMBOL);
            instance.Add(BINARY);
            instance.Add(DATE_TIME);
            instance.Add(DATE_TIME_EPOCH_MS);
            return instance;
        }

        public static JournalColumnRegistry Instance
        {
            get { return INSTANCE.Value; }
        }

        public void Add(IColumnDataType columnType)
        {
            if (!_byFieldType.TryAdd(columnType.ColumnType, columnType))
            {
                throw new NFSdbArgumentException(string.Format("Column type of '{0}' already exists in the column registry.", columnType.ColumnType));
            }
            _byDataType.TryAdd(columnType.Clazz, columnType);
        }

        public IColumnDataType GetSerializer(EFieldType fieldType)
        {
            switch (fieldType)
            {
                case EFieldType.Byte:
                    return BYTE;
                case EFieldType.Bool:
                    return BOOL;
                case EFieldType.Int16:
                    return INT16;
                case EFieldType.Int32:
                    return INT32;
                case EFieldType.Int64:
                    return INT64;
                case EFieldType.Double:
                    return DOUBLE;
                case EFieldType.String:
                    return STRING;
                case EFieldType.BitSet:
                    return BITSET;
                case EFieldType.Symbol:
                    return SYMBOL;
                case EFieldType.Binary:
                    return BINARY;
                case EFieldType.DateTime:
                    return DATE_TIME;
                case EFieldType.DateTimeEpochMs:
                    return DATE_TIME_EPOCH_MS;
                default:
                    return _byFieldType[fieldType];
            }
        }

        public IColumnDataType GetSerializer(Type dataType)
        {
            IColumnDataType ret;
            _byDataType.TryGetValue(dataType, out ret);
            return ret;
        } 
    }
}