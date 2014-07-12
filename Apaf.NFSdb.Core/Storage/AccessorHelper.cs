using System;
using System.Runtime.InteropServices;

namespace Apaf.NFSdb.Core.Storage
{
    public unsafe class AccessorHelper
    {
        public static SYSTEM_INFO Info;

        static AccessorHelper()
        {
            GetSystemInfo(ref Info);
        }

        public static void ReadData(byte* viewAddress, long offset, byte[] array, int arrayOffset,
            int sizeBytes)
        {
            viewAddress += offset % Info.dwAllocationGranularity;
            fixed (byte* dest = array)
            {
                Memcpy(dest, viewAddress, sizeBytes);
            }
        }

        public static byte ReadByte(byte* viewAddress, long offset)
        {
            viewAddress += offset % Info.dwAllocationGranularity;
            return viewAddress[0];
        }

        internal static void Memcpy(byte* dest, byte* src, int len)
        {
            // AMD64 implementation uses longs instead of ints where possible 
            //
            // <STRIP>This is a faster memcpy implementation, from
            // COMString.cpp.  For our strings, this beat the processor's
            // repeat & move single byte instruction, which memcpy expands into. 
            // (You read that correctly.)
            // This is 3x faster than a simple while loop copying byte by byte, 
            // for large copies.</STRIP> 
            if (len >= 16)
            {
                do
                {
#if AMD64
                    ((long*)dest)[0] = ((long*)src)[0]; 
                    ((long*)dest)[1] = ((long*)src)[1];
#else
                    ((int*)dest)[0] = ((int*)src)[0];
                    ((int*)dest)[1] = ((int*)src)[1];
                    ((int*)dest)[2] = ((int*)src)[2];
                    ((int*)dest)[3] = ((int*)src)[3];
#endif
                    dest += 16;
                    src += 16;
                } while ((len -= 16) >= 16);
            }
            if (len > 0)  // protection against negative len and optimization for len==16*N 
            {
                if ((len & 8) != 0)
                {
#if AMD64
                    ((long*)dest)[0] = ((long*)src)[0];
#else
                    ((int*)dest)[0] = ((int*)src)[0];
                    ((int*)dest)[1] = ((int*)src)[1];
#endif
                    dest += 8;
                    src += 8;
                }
                if ((len & 4) != 0)
                {
                    ((int*)dest)[0] = ((int*)src)[0];
                    dest += 4;
                    src += 4;
                }
                if ((len & 2) != 0)
                {
                    ((short*)dest)[0] = ((short*)src)[0];
                    dest += 2;
                    src += 2;
                }
                if ((len & 1) != 0)
                    *dest++ = *src++;
            }

        } 

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern void GetSystemInfo(ref SYSTEM_INFO lpSystemInfo);

        public struct SYSTEM_INFO
        {
            internal int dwOemId;
            internal int dwPageSize;
            internal IntPtr lpMinimumApplicationAddress;
            internal IntPtr lpMaximumApplicationAddress;
            internal IntPtr dwActiveProcessorMask;
            internal int dwNumberOfProcessors;
            internal int dwProcessorType;
            internal int dwAllocationGranularity;
            internal short wProcessorLevel;
            internal short wProcessorRevision;
        }
    }
}