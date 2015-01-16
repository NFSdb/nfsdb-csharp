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
namespace Apaf.NFSdb.Core.Storage
{
    public class ReadContext : IReadContext
    {
        private byte[] _arr1;

        public byte[] AllocateByteArray(int size)
        {
            return _arr1 ?? (_arr1 = new byte[size]);
        }

        public byte[] AllocateByteArray2(int size)
        {
            return new byte[size];
        }

        public byte[] AllocateByteArray3(int size)
        {
            return new byte[size];
        }
    }
}