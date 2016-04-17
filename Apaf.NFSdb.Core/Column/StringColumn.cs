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
using System.Diagnostics;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public class StringColumn : BinaryColumn, IStringColumn
    {
        public StringColumn(IRawFile data, IRawFile index, 
            int maxSize, string propertyName)
            : base(data, index, maxSize, propertyName)
        {
        }

        public unsafe string GetString(long rowID, ReadContext readContext)
        {
            var byteArray = GetBytes(rowID, readContext);
            if (byteArray == null)
            {
                return null;
            }
            var charlen = byteArray.Length/2;

#if BIGENDIAN
            fixed (byte* src = byteArray)
            {
                for (int i = 0; i < charlen * 2; i+=2)
                {
                    byte t = src[i];
                    src[i] = src[i + 1];
                    src[i + 1] = t;
                }
                var srcChar = (char*)src;
                var str = new string(srcChar, 0, charlen);
                return str;
            }
#else
            fixed (byte* src = byteArray)
            {
                var srcChar = (char*)src;
                return new string(srcChar, 0, charlen);
            }
#endif

        }

        public unsafe void SetString(long rowID, string value, PartitionTxData tx)
        {
            if (value != null)
            {
#if BIGENDIAN
                var byteArray = tx.ReadCache.AllocateByteArray2(value.Length * 2);
                fixed (byte* src = byteArray)
                {
                    var charlen = value.Length;
                    int pos = 0;
                    fixed (char* chars = value)
                    {
                        var strBytes = (byte*) &chars[0];
                        for (int i = 0; i < charlen; i++)
                        {
                            src[pos++] = strBytes[2 * i + 1];
                            src[pos++] = strBytes[2 * i];
                        }
                    }
                    DebugCheckEquals(pos, charlen * 2);
                }

                SetBytes(rowID, byteArray, tx);
#else
                fixed (char* chars = value)
                {
                    var strBytes = (byte*)chars;
                    SetBytes(rowID, strBytes, value.Length * 2, tx);
                }
#endif
            }
            else
            {
                SetBytes(rowID, null, 0, tx);
            }
        }

        public override object GetValue(long rowID, ReadContext readContext)
        {
            return GetString(rowID, readContext);
        }

        public override void SetValue(long rowID, object value, PartitionTxData readContext)
        {
            SetString(rowID, (string)value, readContext);
        }

        // ReSharper disable once UnusedMember.Local
        // ReSharper disable UnusedParameter.Local
        [Conditional("DEBUG")]
        private void DebugCheckEquals(int pos, int size)
        {
            if (pos != size)
            {
                throw new NFSdbUnsafeDebugCheckException("Write string byte array size check failed");
            }
        }
        // ReSharper restore UnusedParameter.Local


        protected override void WriteLength(long writeOffset, int size)
        {
            base.WriteLength(writeOffset, size / 2);
        }

        protected override int ReadLength(IRawFile headerBuff, long offset)
        {
            return base.ReadLength(headerBuff, offset)*2;
        }

        public override EFieldType FieldType
        {
            get { return EFieldType.String; }
        }

        string ITypedColumn<string>.Get(long rowID, ReadContext readContext)
        {
            return GetString(rowID, readContext);
        }
    }
}