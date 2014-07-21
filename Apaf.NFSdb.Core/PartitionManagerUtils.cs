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
using System.Diagnostics;
using System.Globalization;
using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Exceptions;

namespace Apaf.NFSdb.Core
{
    public static class PartitionManagerUtils
    {
        public static DateTime GetPartitionEndDate(DateTime startDate,
            EPartitionType partitionType)
        {
            DebugCheckDate(startDate);

            switch (partitionType)
            {
                case EPartitionType.Day:
                    return startDate.AddDays(1);
                case EPartitionType.Month:
                    return startDate.AddMonths(1);
                case EPartitionType.Year:
                    return startDate.AddYears(1);
                default:
                    return DateTime.MaxValue;
            }
        }

        public static DateTime GetPartitionStartDate(DateTime timestamp,
            EPartitionType partitionType)
        {
            switch (partitionType)
            {
                case EPartitionType.Day:
                    return timestamp.Date;
                case EPartitionType.Month:
                    return new DateTime(timestamp.Year, timestamp.Month, 1);
                case EPartitionType.Year:
                    return new DateTime(timestamp.Year, 1, 1);;
                default:
                    return DateTime.MinValue;
            }
        }

        public static string GetPartitionDirName(DateTime timestamp,
            EPartitionType partitionType)
        {
            switch (partitionType)
            {
                case EPartitionType.Day:
                    return timestamp.ToString("yyyy-MM-dd");
                case EPartitionType.Month:
                    return timestamp.ToString("yyyy-MM");
                case EPartitionType.Year:
                    return timestamp.ToString("yyyy");
                default:
                    return MetadataConstants.DEFAULT_PARTITION_DIR;
            }
        }

        [Conditional("DEBUG")]
        private static void DebugCheckDate(DateTime startDate)
        {
            if (startDate != startDate.Date)
            {
                throw new NFSdbPartitionException("Partition date is not supposed to have time component");
            }
        }

        public static DateTime? ParseDateFromDirName(string subDir, EPartitionType partitionType)
        {
            var dateString = subDir;
            switch (partitionType)
            {
                case EPartitionType.Day:
                    break;
                case EPartitionType.Month:
                    dateString += "-01";
                    break;
                case EPartitionType.Year:
                    dateString += "-01-01";
                    break;
                default:
                    if (MetadataConstants.DEFAULT_PARTITION_DIR
                        .Equals(subDir, StringComparison.OrdinalIgnoreCase))
                    {
                        return DateTime.MinValue;
                    }
                    return null;
            }
            DateTime date;
            if (!DateTime.TryParseExact(dateString, "yyyy-MM-dd", 
                CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out date))
            {
                return null;
            }
            return date.Date;
        }
    }
}