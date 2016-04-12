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

using System.Collections;
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

        public static void SetToIndex(this ArrayList list, int index, object value)
        {
            if (list.Count > index)
            {
                list[index] = value;
            }
            else
            {
                while (list.Count < index)
                {
                    list.Add(null);
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
