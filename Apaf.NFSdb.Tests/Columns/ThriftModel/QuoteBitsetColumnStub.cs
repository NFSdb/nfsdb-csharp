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
using System.Linq;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Tests.Columns.ThriftModel
{
    public class QuoteBitsetColumnStub : IBitsetColumn
    {
        private readonly int _byteArraySize;
        private readonly IColumn[] _columns;
        private byte[] _columnByteValue;
        private int[] _setCols;

        public QuoteBitsetColumnStub(IColumn[] columns, int[] setCols)
        {
            _columns = columns;
            _setCols = setCols;
            FieldType = EFieldType.BitSet;
            _byteArraySize = _columns.Length/8 + 1;

            _columnByteValue = BuildValue();
        }

        public int[] SetColumnIndecies
        {
            get
            {
                var byteSetter =
                    new ByteArray(_columnByteValue);

                _setCols = Enumerable.Range(0, _columns.Length).
                    Where(byteSetter.IsSet).ToArray();
                return _setCols;
            }
        }

        public byte[] GetValue(long rowID, IReadContext read)
        {
            return _columnByteValue;
        }

        public void SetValue(long rowID, byte[] bitArray, ITransactionContext readContext)
        {
            _columnByteValue = bitArray;
        }

        public int GetByteSize()
        {
            return _byteArraySize;
        }

        public EFieldType FieldType { get; private set; }
        public string PropertyName { get; private set; }

        private byte[] BuildValue()
        {
            var bytes = new byte[_byteArraySize];
            var byteSetter = new ByteArray(bytes);
            
            // By default byteSetter has values set to true
            //for (int i = 0; i < _columns.Length; i++)
            //{
            //    byteSetter.Set(i, false);
            //}

            foreach (int index in _setCols)
            {
                byteSetter.Set(index, true);
            }
            return bytes;
        }

        public ByteArray Get(long rowID, IReadContext readContext)
        {
            return new ByteArray(GetValue(rowID, readContext));
        }
    }
}