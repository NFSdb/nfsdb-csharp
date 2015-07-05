using System.Collections.Generic;

namespace Apaf.NFSdb.Core.Collections
{
    public static class ListExtensions
    {
        public static void SetToIndex<T>(this IList<T> list, int index, T value)
        {
            if (list.Count > index)
            {
                list[index] = value;
            }
            else 
            {
                while (list.Count < index)
                {
                    list.Add(default(T));
                }

                list.Add(value);
            }
        }

        public static T LastNotNull<T>(this IList<T> list) where T : class 
        {
            for (int i = list.Count - 1; i >= 0; i--)
            {
                var item = list[i];
                if (item != null) return item;
            }
            return null;
        }
    }
}