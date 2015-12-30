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
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class SymbolFilter<T> : ColumnFilterBase<T>
    {
        private readonly IColumnMetadata _column;
        private readonly T _value;
        private readonly IList<T> _values;
        private HashSet<T> _valuesHash;

        public SymbolFilter(IColumnMetadata column, T value)
            : base(column)
        {
            _column = column;
            _value = value;
        }

        public SymbolFilter(IColumnMetadata column, IList<T> values)
            : base(column)
        {
            if (values == null) throw new ArgumentNullException("values");

            _column = column;
            if (values.Count == 1)
            {
                _value = values[0];
            }
            else
            {
                _values = values;
            }
        }

        public IList<T> FilterValues
        {
            get
            {
                if (_values != null)
                {
                    return _values;
                }
                return new[] {_value};
            }
        }

        protected override bool IsMatch(T value)
        {
            if (_values == null)
            {
                return Equals(value, _value);
            }

            if (_valuesHash == null)
            {
                _valuesHash = new HashSet<T>(_values);
            }
            return _valuesHash.Contains(value);
        }

        protected override SingleMultipleValues<T> GetAllMatchingValues(IReadTransactionContext tx)
        {
            if (_values != null)
            {
                return SingleMultipleValues<T>.Multiple(_values);
            }
            return SingleMultipleValues<T>.Single(_value);
        }

        public override long GetCardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            return journal.QueryStatistics.GetCardinalityByColumnValue(tx, _column, _values ?? new[] {_value});
        }

        public override string ToString()
        {
            if (_values != null)
            {
                return string.Format("{0} in ({1})", _column.PropertyName, string.Join(",", _values));
            }
            return string.Format("{0} = {1}", _column.PropertyName, _value);
        }
    }
}