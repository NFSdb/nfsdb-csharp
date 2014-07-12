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
using System;
using System.Text;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Tx
{
    public class TxLog : ITxLog
    {
        private readonly IRawFile _data;
        public static readonly long MIN_TX_ADDRESS = 9;

        public TxLog(IRawFile data)
        {
            _data = data;
            if (data == null) throw new ArgumentNullException("data");
        }

        public bool IsEmpty()
        {
            return _data.GetAppendOffset() <= MIN_TX_ADDRESS;
        }

        public TxRec Get()
        {
            if (IsEmpty()) return null;

            var tx = new TxRec();
            var offset = GetTxAddress();

            int txSize = ReadInt32(ref offset);
            tx.PrevTxAddress = ReadInt64(ref offset);

            tx.Command = ReadByte(ref offset);
            tx.Timestamp = ReadInt64(ref offset);
            tx.JournalMaxRowID = ReadInt64(ref offset);
            tx.LastPartitionTimestamp = ReadInt64(ref offset);
            tx.LagSize = ReadInt64(ref offset);

            var sz = ReadByte(ref offset);
            if (sz == 0)
            {
                tx.LagName = null;
            }
            else
            {
                // lagName
                sz = ReadByte(ref offset);
                var nameBytes = new byte[sz];
                _data.ReadBytes(offset, nameBytes, 0, sz);
                tx.LagName = Encoding.ASCII.GetString(nameBytes);

                offset += sz;
            }

            // symbolTableSizes
            tx.SymbolTableSizes = ReadInt32Array(ref offset);

            //symbolTableIndexPointers
            tx.SymbolTableIndexPointers = ReadInt64Array(ref offset);

            //indexPointers
            tx.IndexPointers = ReadInt64Array(ref offset);

            //lagIndexPointers
            tx.LagIndexPointers = ReadInt64Array(ref offset);

            return tx;
        }

        public void Create(TxRec tx)
        {
            if (tx.LagName != null && tx.LagName.Length > 64)
            {
                throw new NFSdbTransactionStateExcepton("Partition name is too long");
            }

            var offset = Math.Max(MIN_TX_ADDRESS, _data.GetAppendOffset());
            var orignalOffset = offset;

            // 4
            var size = tx.Size();
            _data.WriteInt32(offset, size);
            offset += 4;

            // 8
            WriteInt64(tx.PrevTxAddress, ref offset);

            // 1
            WriteByte(tx.Command, ref offset);
            // 8
            WriteInt64(DateTime.Now.Ticks, ref offset);
            // 8
            WriteInt64(tx.JournalMaxRowID, ref offset);
            // 8
            WriteInt64(tx.LastPartitionTimestamp, ref offset);
            // 8
            WriteInt64(tx.LagSize, ref offset);
            // 1
            if (tx.LagName == null)
            {
                WriteByte(0, ref offset);
            }
            else
            {
                WriteByte(1, ref offset);
                // 2
                WriteByte((byte) tx.LagName.Length, ref offset);
                // TxRec.lagName.len
                foreach (char t in tx.LagName)
                {
                    WriteByte((byte)t, ref offset);
                }
            }
            // 2 + 4 * TxRec.symbolTableSizes.len
            WriteInt32Array(tx.SymbolTableSizes, ref offset);
            WriteInt64Array(tx.SymbolTableIndexPointers, ref offset);
            WriteInt64Array(tx.IndexPointers, ref offset);
            WriteInt64Array(tx.LagIndexPointers, ref offset);

            // write out TxRec address
            SetTxAddress(orignalOffset);
            _data.SetAppendOffset(orignalOffset + size);
        }

        private void SetTxAddress(long address)
        {
            _data.WriteInt64(0, address);
            _data.WriteByte(8, GetCheckByte(address));
        }

        private long GetTxAddress()
        {
            long oldAddress = -1;
            byte oldCheck = 0;
            while (true)
            {
                var address = _data.ReadInt64(0);
                var check = _data.ReadByte(8);

                if (address == oldAddress && check == oldCheck)
                {
                    throw new NFSdbInvalidTxAddressException(address,
                        "Check some does not match address value and does not change on re-read");
                }

                var actual = GetCheckByte(address);
                if (actual == check)
                {
                    return address;
                }
                oldAddress = address;
                oldCheck = check;
            }
        }

        private static unsafe byte GetCheckByte(long address)
        {
            byte actual;
            var addPtr = &address;
            var bytePtr = (byte*) addPtr;
            actual = bytePtr[0];
            for (int i = 1; i < 8; i++)
            {
                actual ^= bytePtr[i];
            }
            return actual;
        }

        private void WriteInt32Array(int[] array, ref long offset)
        {
            if (array != null)
            {
                WriteUInt16((uint) array.Length, ref offset);
                foreach (var item in array)
                {
                    _data.WriteInt32(offset, item);
                    offset += 4;
                }
            }
            else
            {
                WriteUInt16(0, ref offset);
            }
        }

        private void WriteInt64Array(long[] array, ref long offset)
        {
            if (array != null)
            {
                WriteUInt16((uint) array.Length, ref offset);
                foreach (var item in array)
                {
                    _data.WriteInt64(offset, item);
                    offset += 8;
                }
            }
            else
            {
                WriteUInt16(0, ref offset);
            }
        }

        private void WriteUInt16(uint value, ref long offset)
        {
            _data.WriteUInt16(offset, value);
            offset += 2;
        }

        private void WriteInt64(long value, ref long offset)
        {
            _data.WriteInt64(offset, value);
            offset += 8;
        }

        private void WriteInt32(int value, ref long offset)
        {
            _data.WriteInt64(offset, value);
            offset += 4;
        }

        private void WriteByte(byte value, ref long offset)
        {
            _data.WriteByte(offset, value);
            offset += 1;
        }

        private byte ReadByte(ref long offset)
        {
            var res = _data.ReadByte(offset);
            offset += 1;
            return res;
        }

        private long ReadInt64(ref long offset)
        {
            var res = _data.ReadInt64(offset);
            offset += 8;
            return res;
        }

        private int ReadInt32(ref long offset)
        {
            var res = _data.ReadInt32(offset);
            offset += 4;
            return res;
        }

        private int[] ReadInt32Array(ref long offset)
        {
            int sz = _data.ReadUInt16(offset);
            offset += 2;
            var lagIndexPointers = new int[sz];
            for (int i = 0; i < sz; i++)
            {
                lagIndexPointers[i] = _data.ReadInt32(offset);
                offset += 4;
            }
            return lagIndexPointers;
        }

        private long[] ReadInt64Array(ref long offset)
        {
            int sz = _data.ReadUInt16(offset);
            offset += 2;
            var lagIndexPointers = new long[sz];
            for (int i = 0; i < sz; i++)
            {
                lagIndexPointers[i] = _data.ReadInt64(offset);
                offset += 8;
            }
            return lagIndexPointers;
        }
    }
}