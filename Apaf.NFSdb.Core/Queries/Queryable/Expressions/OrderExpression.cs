﻿#region copyright
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

using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class OrderExpression : PostResultExpression
    {
        private readonly LambdaExpression _predicate;

        public OrderExpression(Expression body, EJournalExpressionType operation)
            : base(body, operation)
        {
        }

        public OrderExpression(Expression body, EJournalExpressionType operation, LambdaExpression predicate)
            : base(body, operation)
        {
            _predicate = predicate;
        }

        public LambdaExpression Predicate
        {
            get { return _predicate; }
        }
    }
}