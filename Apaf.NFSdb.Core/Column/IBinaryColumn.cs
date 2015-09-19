﻿#region copyright
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
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Column
{
    public interface IBinaryColumn : IRefTypeColumn, ITypedColumn<byte[]>
    {
        byte[] GetBytes(long rowID, IReadContext readContext);
        unsafe int GetBytes(long rowID, byte* value, int startIndex, IReadContext readContext);

        void SetBytes(long rowID, byte[] value, ITransactionContext readContext);
        unsafe void SetBytes(long rowID, byte* value, int length, ITransactionContext readContext);

        unsafe void SetBytes(long rowID, byte* value, int length, PartitionTxData readContext);
    }
}