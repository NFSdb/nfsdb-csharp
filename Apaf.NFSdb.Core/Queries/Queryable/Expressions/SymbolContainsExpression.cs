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
using System.Collections;
using System.Linq;
using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class SymbolContainsExpression : Expression
    {
        private readonly Expression _match;
        private readonly IEnumerable _values;

        public SymbolContainsExpression(Expression match, IEnumerable values)
        {
            _match = match;
            _values = values;
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)EJournalExpressionType.Contains; }
        }

        public override Type Type
        {
            get { return typeof(bool); }
        }

        public Expression Match
        {
            get { return _match; }
        }

        public IEnumerable Values
        {
            get { return _values; }
        }

        public override string ToString()
        {
            return string.Format("{0} IN ({1})", Match,
                string.Join(", ", Values.Cast<object>().Select(v => v.ToString())));
        }
    }
}