#region copyright
/*
 * Copyright (c) 2014. APAF (Alex Pelagenko).
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
namespace Apaf.NFSdb.Core.Column
{
    public struct ByteArray
    {
        private readonly byte[] _data;

        public ByteArray(byte[] data)
        {
            _data = data;
        }

        public bool IsSet(int index)
        {
            return (_data[index / 8] & (byte)(1 << (index % 8))) != 0;
        }

        public void Set(int index, bool value)
        {
            if (value)
            {
                _data[index / 8] |= (byte)(1 << (index % 8));
            }
            else
            {
                _data[index / 8] &= (byte)~(1 << (index % 8));
            }
        }
    }
}