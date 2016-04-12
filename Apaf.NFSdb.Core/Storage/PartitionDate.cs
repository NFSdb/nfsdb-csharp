using System;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Storage
{
    public struct PartitionDate
    {
        public readonly EPartitionType PartitionType;
        public readonly DateTime Date;
        public readonly int Version;
        private string _name;

        public PartitionDate(DateTime date, int version, EPartitionType partitionType)
            : this()
        {
            PartitionType = partitionType;

            switch (PartitionType)
            {
                case EPartitionType.Day:
                    Date = date.Date;
                    break;
                case EPartitionType.Month:
                    Date = new DateTime(date.Year, date.Month, 1);
                    break;
                case EPartitionType.Year:
                    Date = new DateTime(date.Year, 1, 1);
                    break;
                default:
                    Date = DateTime.MinValue;
                    break;
            }

            Version = version;
        }

        public PartitionDate(DateTime date, int version, EPartitionType partitionType, string name)
            : this()
        {
            PartitionType = partitionType;
            Date = date;
            Version = version;
            _name = name;
        }

        public string Name
        {
            get { return _name ?? (_name = BuildName()); }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is PartitionDate && Equals((PartitionDate) obj);
        }

        public bool Equals(PartitionDate other)
        {
            return PartitionType == other.PartitionType && Date.Equals(other.Date) && Version == other.Version;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (int)PartitionType;
                hashCode = (hashCode * 397) ^ Date.GetHashCode();
                hashCode = (hashCode * 397) ^ Version;
                return hashCode;
            }
        }

        public override string ToString()
        {
            return Name;
        }

        private string BuildName()
        {
            string baseDirName;
            switch (PartitionType)
            {
                case EPartitionType.Day:
                    baseDirName = Date.ToString(MetadataConstants.PARTITION_DATE_FORMAT);
                    break;
                case EPartitionType.Month:
                    baseDirName = Date.ToString(MetadataConstants.PARTITION_MONTH_FORMAT);
                    break;
                case EPartitionType.Year:
                    baseDirName = Date.ToString(MetadataConstants.PARTITION_YEAR_FORMAT);
                    break;
                default:
                    baseDirName = MetadataConstants.DEFAULT_PARTITION_DIR;
                    break;
            }

            if (Version > 0)
            {
                return baseDirName + MetadataConstants.PARTITION_VERSION_SEPARATOR + Version;
            }
            return baseDirName;
        }
    }
}