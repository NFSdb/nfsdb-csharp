using System;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class RecordSerializer : IFieldSerializer
    {
        public object Read(long rowID, IReadContext readContext)
        {
            throw new NotSupportedException();
        }

        public void Write(object item, long rowID, PartitionTxData readContext)
        {
            throw new NotImplementedException();
        }
    }
}