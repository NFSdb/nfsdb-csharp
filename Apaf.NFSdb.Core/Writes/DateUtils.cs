using System;

namespace Apaf.NFSdb.Core.Writes
{
    public static class DateUtils
    {
        private static DateTime _epoch = new DateTime(1970, 1, 1, 0, 0, 0, 0);

        public static DateTime UnixTimestampToDateTime(long unixTimeStamp)
        {
            var dtDateTime = _epoch.AddMilliseconds(unixTimeStamp);
            return dtDateTime;
        }

        public static long DateTimeToUnixTimeStamp(DateTime dateTime)
        {
            return (dateTime.Ticks - _epoch.Ticks) / TimeSpan.TicksPerMillisecond;
        }
    }
}