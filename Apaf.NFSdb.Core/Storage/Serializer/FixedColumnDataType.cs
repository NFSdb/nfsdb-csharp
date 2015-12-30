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
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class FixedColumnDataType : IColumnDataType
    {
        internal FixedColumnDataType(EFieldType fieldType, Type clazz)
        {
            ColumnType = fieldType;
            Clazz = clazz;
            ColumnTypeName = fieldType.ToString();
        }

        public Type Clazz { get; private set; }
        public EFieldType ColumnType { get; private set; }
        public string ColumnTypeName { get; private set; }

        public IComparer<long> GetColumnComparer(int columnId, IReadTransactionContext tx, bool ascending)
        {
            switch (ColumnType)
            {
                case EFieldType.Byte:
                    return new ColumnValueComparer<byte>(columnId, tx, ascending);
                case EFieldType.Bool:
                    return new ColumnValueComparer<bool>(columnId, tx, ascending);
                case EFieldType.Int16:
                    return new ColumnValueComparer<short>(columnId, tx, ascending);
                case EFieldType.Int32:
                    return new ColumnValueComparer<int>(columnId, tx, ascending);
                case EFieldType.Int64:
                    return new ColumnValueComparer<long>(columnId, tx, ascending);
                case EFieldType.Double:
                    return new ColumnValueComparer<double>(columnId, tx, ascending);
                case EFieldType.Symbol:
                case EFieldType.String:
                    return new ColumnValueComparer<string>(columnId, tx, ascending);
                case EFieldType.DateTime:
                case EFieldType.DateTimeEpochMs:
                    return new ColumnValueComparer<DateTime>(columnId, tx, ascending);
                case EFieldType.Binary:
                    return new BinaryColumnValueComparer(columnId, tx, ascending);

                default:
                    throw QueryExceptionExtensions.NotSupported("Column {0} of type {1} is not sortable.", columnId, columnId);
            }
        }

        public bool IsFixedSize()
        {
            switch (ColumnType)
            {
                case EFieldType.Byte:
                case EFieldType.Bool:
                case EFieldType.Int32:
                case EFieldType.Int64:
                case EFieldType.Int16:
                case EFieldType.Double:
                case EFieldType.DateTime:
                    return true;
                default:
                    return false;
            }
        }

        public int Size
        {
            get
            {
                switch (ColumnType)
                {
                    case EFieldType.Byte:
                        return 1;
                    case EFieldType.Bool:
                        return 1;
                    case EFieldType.Int32:
                        return 4;
                    case EFieldType.Int64:
                        return 8;
                    case EFieldType.Int16:
                        return 2;
                    case EFieldType.Double:
                        return 8;
                    case EFieldType.DateTime:
                    case EFieldType.DateTimeEpochMs:
                        return 8;

                    default:
                        throw new NFSdbArgumentException("Column of type " + ColumnTypeName + " is not a fixed size column.");
                }
            }
        }

        public object ToTypedValue(object literal)
        {
            if (literal == null)
            {
                switch (ColumnType)
                {
                    case EFieldType.Symbol:
                    case EFieldType.String:
                        break;
                    default:
                        throw new NFSdbArgumentException(
                            string.Format("Cannot convert <null> value to column of type '{0}'", ColumnType));

                }
            }
            switch (ColumnType)
            {
                case EFieldType.Byte:
                    return Convert.ToByte(literal);
                case EFieldType.Bool:
                    return (bool)literal;
                case EFieldType.Int16:
                    return Convert.ToInt16(literal);
                case EFieldType.Int32:
                    return Convert.ToInt32(literal);
                case EFieldType.Int64:
                    return Convert.ToInt64(literal);
                case EFieldType.Double:
                    return Convert.ToDouble(literal);
                case EFieldType.Symbol:
                case EFieldType.String:
                    return literal;
                case EFieldType.DateTime:
                case EFieldType.DateTimeEpochMs:
                    return (DateTime)literal;

                default:
                    throw new NFSdbArgumentException(
                        string.Format("Cannot convert object to column of type '{0}'", ColumnType));
            }

        }
    }
}