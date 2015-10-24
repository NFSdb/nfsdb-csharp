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
namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public enum EJournalExpressionType
    {
        Contains = 50000,
        Single = 50001,
        First = 50002,
        Last = 50003,
        Reverse = 50004,
        LongCount = 50005,
        Count = 50006,
        OrderBy = 50007,
        ThenOrderBy = 50008,
        OrderByDescending = 50009,
        ThenOrderByDescending = 50010,
        Take = 50011,
        Skip = 50012,
        LatestBy = 50013,
        Intersect = 50014,
        Union = 50015,
        Filter = 50016,
    }
}