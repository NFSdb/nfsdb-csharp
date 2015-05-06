using System.Collections.Generic;

namespace Apaf.NFSdb.Core.Tx
{
    public class PartitionMetadata
    {
        public Dictionary<string, int> FileIDs;
        public int[] FileRowBlockBitHints;
        public int[] BitHints; 
    }
}