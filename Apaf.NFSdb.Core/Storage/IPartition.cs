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

using System;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IPartition : IPartitionReader, IDisposable
    {
        string DirectoryPath { get; }
        void TryCloseFiles();
        int AddRef();
        int RemoveRef(int partitionOffloadMs);
        void SaveConfig(IReadTransactionContext tx = null);

        DateTime StartDate { get; }
        int Version { get; }
        DateTime EndDate { get; }

        void Append(object item, PartitionTxData tx);
        void MarkOverwritten();
        bool IsOverwritten { get; }
    }
}