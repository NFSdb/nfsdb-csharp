using System;

namespace Apaf.NFSdb.Core.Exceptions
{
    public class NFsdbFileMapSizeException : NFSdbBaseExcepton
    {
        public long FileSize { get; set; }
        public long RequestedStart { get; set; }
        public long RequestedSize { get; set; }

        internal NFsdbFileMapSizeException(string fileName, long fileSize, long requestedStart, long requestedSize)
            : base(string.Format("Errorm mapping file "))
        {
            FileSize = fileSize;
            RequestedStart = requestedStart;
            RequestedSize = requestedSize;
        }
    }
}