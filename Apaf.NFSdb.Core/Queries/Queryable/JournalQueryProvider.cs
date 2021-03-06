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
using System;
using System.Linq;
using System.Linq.Expressions;
using Apaf.NFSdb.Core.Queries.Queryable.Expressions;
using Apaf.NFSdb.Core.Tx;
using IQToolkit;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class JournalQueryProvider<T> : QueryProvider
    {
        private readonly IJournalCore _journal;
        private readonly IReadTransactionContext _tx;
        private QueryCache _cache;

        public JournalQueryProvider(IJournalCore journal, IReadTransactionContext tx)
        {
            _journal = journal;
            _tx = tx;
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
            var res = result.Build();

            if (res.IsSingle)
            {
                var rowID = res.RowID;
                
                // First / Last OrDefault can return null.
                if (rowID == 0) return null;
                int partitionID = RowIDUtil.ToPartitionIndex(rowID);
                var lastPartitionReader = _tx.Read(partitionID);
                long localRowID = RowIDUtil.ToLocalRowID(rowID);

                // ReSharper disable once PossibleNullReferenceException
                return lastPartitionReader.Read<T>(localRowID, _tx.ReadCache);
            }

            switch (res.PostExpression)
            {
                case EJournalExpressionType.None:
                    return new ResultSet<T>(res.Rowids, _tx);
                case EJournalExpressionType.LongCount:
                    return res.Rowids.LongCount();
                case EJournalExpressionType.Count:
                    return res.Rowids.Count();
                default:
                    throw QueryExceptionExtensions.ExpressionNotSupported(
                        string.Format("Post expression {0} is not supported at this level", res.PostExpression), expression);
            }
        }

        protected virtual QueryPlanBinder<T> CreateTranslator()
        {
            return new QueryPlanBinder<T>(_journal, _tx);
        }

        private ResultSetBuilder GetExecutionPlan(Expression expression)
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