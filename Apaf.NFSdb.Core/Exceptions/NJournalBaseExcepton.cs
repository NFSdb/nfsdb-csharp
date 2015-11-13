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

namespace Apaf.NFSdb.Core.Exceptions
{
    [Serializable]
    public abstract class NFSdbBaseExcepton : Exception
    {
        protected NFSdbBaseExcepton()
        {
        }

        protected NFSdbBaseExcepton(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        protected NFSdbBaseExcepton(string message, Exception inException, params object[] args)
            : base(string.Format(message, args), inException)
        {
        }

        protected NFSdbBaseExcepton(string message, Exception inException)
            : base(message, inException)
        {
        }
    }
}