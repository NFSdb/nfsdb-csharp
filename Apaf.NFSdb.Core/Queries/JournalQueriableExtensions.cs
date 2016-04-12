using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Apaf.NFSdb.Core.Queries
{
    public static class JournalQueriableExtensions
    {
        private static MethodInfo GetMethodInfo<T1, T2, T3>(Func<T1, T2, T3> f, T1 a1, T2 a2)
        {
            return f.Method;
        }
 
        public static IQueryable<TSource> LatestBy<TSource, TKey>(this IQueryable<TSource> source, 
            Expression<Func<TSource, TKey>> keySelector)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }

            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    GetMethodInfo(LatestBy, source, keySelector),
                    new[] {source.Expression, Expression.Quote(keySelector)})
                );
        }

        internal static IQueryable<TSource> ByPartitionId<TSource>(this IQueryable<TSource> source, int partition)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }

            return source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    GetMethodInfo(ByPartitionId, source, partition),
                    new[] { source.Expression, Expression.Constant(partition) })
                );
        }
    }
}