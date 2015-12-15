namespace Apaf.NFSdb.Core.Storage.Serializer.Records
{
    public struct Record
    {
        public readonly IRecordSet RecordSet;
        public readonly long RowId;

        public Record(IRecordSet recordSet, long rowId)
        {
            RecordSet = recordSet;
            RowId = rowId;
        }
    }
}