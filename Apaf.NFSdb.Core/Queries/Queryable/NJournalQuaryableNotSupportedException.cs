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
using Apaf.NFSdb.Core.Exceptions;

namespace Apaf.NFSdb.Core.Queries.Queryable
{
    public class NFSdbQueryableNotSupportedException : NFSdbBaseExcepton
    {
        public static NFSdbQueryableNotSupportedException Create(string message, Exception ex)
        {
            return new NFSdbQueryableNotSupportedException(message, ex);
        }

        public static NFSdbQueryableNotSupportedException Create(string message)
        {
            return new NFSdbQueryableNotSupportedException(message);
        }

        public static NFSdbQueryableNotSupportedException Create(string message, QlToken queryToken)
        {
            return new NFSdbQueryableNotSupportedException(message, queryToken);
        }

        private NFSdbQueryableNotSupportedException(string message)
            : base(message)
        {
        }

        private NFSdbQueryableNotSupportedException(string message, Exception ex)
            : base(message, ex)
        {
        }


        private NFSdbQueryableNotSupportedException(string message, QlToken queryToken)
            : base(message)
        {
            QueryToken = queryToken;
        }

        public QlToken QueryToken { get; private set; }
    }
}