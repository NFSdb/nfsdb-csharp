﻿using System.Collections.Generic;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Tx
{
    public class TxReusableState
    {
        public PartitionTxData[] PartitionDataStorage;
        public IReadContext ReadContext;
        public List<IPartitionCore> Partitions;
        public List<bool> Locks;
    }
}