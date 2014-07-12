using System;
using System.Linq;
using System.Linq.Expressions;
using IQToolkit;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class JournalQueryable<T> : IQToolkit.Query<T>
    {
        public JournalQueryable(IQueryProvider provider) : 
            base(provider)
        {
        }

        public JournalQueryable(IQueryProvider provider, Type staticType) : 
            base(provider, staticType)
        {
        }

        public JournalQueryable(QueryProvider provider, Expression expression) : 
            base(provider, expression)
        {
        }
    }
}