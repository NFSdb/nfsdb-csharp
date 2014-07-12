using System;

namespace Apaf.NFSdb.Core.Writes
{
    public class WriterState<T> 
    {
        private readonly Func<T, long> _timestampReader;

        public WriterState(IJournalMetadata<T> metadata)
        {
            _timestampReader = metadata.GetTimestampReader();
        }

        public WriterState(Func<T, long> metadata)
        {
            _timestampReader = metadata;
        }

        public Func<T, long> GetTimestampDelegate
        {
            get
            {
                return _timestampReader;
            }
        }
    }
}