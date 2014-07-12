#region copyright
/*
 * Copyright (c) 2014. APAF (Alex Pelagenko).
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
using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class SymbolContainsExpression : Expression
    {
        private readonly Expression _match;
        private readonly string[] _values;

        public SymbolContainsExpression(Expression match, string[] values)
        {
            _match = match;
            _values = values;
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)JournalExpressionType.Contains; }
        }

        public override Type Type
        {
            get { return typeof(bool); }
        }

        public Expression Match
        {
            get { return _match; }
        }

        public string[] Values
        {
            get { return _values; }
        }
    }
}