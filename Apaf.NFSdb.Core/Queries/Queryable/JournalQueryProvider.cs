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
using System.Linq.Expressions;
using Apaf.NFSdb.Core.Tx;
using IQToolkit;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class JournalQueryProvider<T> : QueryProvider
    {
        private readonly IJournal<T> _journal;
        private readonly IReadTransactionContext _tx;
        private string _latestBySymbol;
        private QueryCache _cache;

        public JournalQueryProvider(IJournal<T> journal, IReadTransactionContext tx)
        {
            _journal = journal;
            _tx = tx;
        }

        public static JournalQueryProvider<T> LatestBy(string symbolName, IJournal<T> journal,
            IReadTransactionContext tx)
        {
            var provider = new JournalQueryProvider<T>(journal, tx);
            provider._latestBySymbol = symbolName;
            return provider;
        }

        public override string GetQueryText(Expression expression)
        {
            throw new NotSupportedException();
        }

        public QueryCache Cache
        {
            get { return _cache; }
            set { _cache = value; }
        }

        public override object Execute(Expression expression)
        {
            var lambda = expression as LambdaExpression;
            if (lambda == null && _cache != null && expression.NodeType != ExpressionType.Constant)
            {
                return _cache.Execute(expression);
            }

            var result = GetExecutionPlan(expression);
            if (_latestBySymbol != null)
            {
                result.TakeLatestBy(_latestBySymbol);
            }
            return result.Build();
        }

        protected virtual QueryPlanBinder<T> CreateTranslator()
        {
            return new QueryPlanBinder<T>(_journal, _tx);
        }

        private ResultSetBuilder<T> GetExecutionPlan(Expression expression)
        {
            // strip off lambda for now
            var lambda = expression as LambdaExpression;
            if (lambda != null)
                expression = lambda.Body;

            var translator = CreateTranslator();

            // translate query into client & server parts
            var result = translator.Build(expression);
            return result;
        }
    }
}