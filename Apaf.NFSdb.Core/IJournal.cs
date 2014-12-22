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
using System.Collections.Generic;
using Apaf.NFSdb.Core.Configuration;
using Apaf.NFSdb.Core.Queries;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Writes;

namespace Apaf.NFSdb.Core
{
    public interface IJournal<T> : IDisposable, IJournalCore
    {
        T Read(long rowID, IReadContext readContext);
        IEnumerable<T> Read(IEnumerable<long> rowIDs, IReadContext readContext);
        IComparer<long> GetRecordsComparer(int[] columnIndices);
        IQuery<T> OpenReadTx();
        IWriter<T> OpenWriteTx();

        IJournalMetadata<T> Metadata { get; }
        IEnumerable<IPartition<T>> Partitions { get; }

        IJournalDiagnostics Diagnostics {get;}
    }
}