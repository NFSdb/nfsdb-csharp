﻿#region copyright
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

namespace Apaf.NFSdb.Core.Writes
{
    public static class DateUtils
    {
        private static DateTime _epoch = new DateTime(1970, 1, 1, 0, 0, 0, 0);
        private const UInt64 TICKS_MASK = UInt64.MaxValue >> 2;

        public static DateTime UnixTimestampToDateTime(long unixTimeStamp)
        {
            var dtDateTime = _epoch.AddMilliseconds(unixTimeStamp);
            return dtDateTime;
        }

        public static long DateTimeToUnixTimeStamp(DateTime dateTime)
        {
            return (dateTime.Ticks - _epoch.Ticks) / TimeSpan.TicksPerMillisecond;
        }

        public unsafe static long ToUnspecifiedDateTicks(DateTime dateTime)
        {
            UInt64 dateNoKind = ((UInt64*)&dateTime)[0] & TICKS_MASK;
            return ((long*)&dateNoKind)[0];
        }
    }
}