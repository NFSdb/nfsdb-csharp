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
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public interface IFixedWidthColumn : IColumn,
        ITypedColumn<int>, 
        ITypedColumn<long>,
        ITypedColumn<short>,
        ITypedColumn<byte>, 
        ITypedColumn<bool>,
        ITypedColumn<double>,
        ITypedColumn<DateTime>
    {
        int GetInt32(long rowID);
        long GetInt64(long rowID);
        short GetInt16(long rowID);
        byte GetByte(long rowID);
        bool GetBool(long rowID);
        double GetDouble(long rowID);
        DateTime GetDateTime(long rowID);

        void SetInt32(long rowID, int value, ITransactionContext readContext);
        void SetInt64(long rowID, long value, ITransactionContext readContext);
        void SetInt16(long rowID, short value, ITransactionContext readContext);
        void SetByte(long rowID, byte value, ITransactionContext readContext);
        void SetBool(long rowID, bool value, ITransactionContext readContext);
        void SetDouble(long rowID, double value, ITransactionContext readContext);
        void SetDateTime(long rowID, DateTime value, ITransactionContext readContext);
    }
}