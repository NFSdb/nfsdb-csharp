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
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Queries
{
    public class LambdaFilter<T> : ColumnFilterBase<T>
    {
        private readonly Func<T, bool> _lambda;

        public LambdaFilter(IColumnMetadata column, Func<T, bool> lambda) 
            : base(column)
        {
            _lambda = lambda;
        }

        protected override bool IsMatch(T value)
        {
            return _lambda(value);
        }

        protected override SingleMultipleValues<T> GetAllMatchingValues(IReadTransactionContext tx)
        {
            return SingleMultipleValues<T>.NONE;
        }

        public override long GetCardinality(IJournalCore journal, IReadTransactionContext tx)
        {
            return long.MaxValue;
        }
    }
}