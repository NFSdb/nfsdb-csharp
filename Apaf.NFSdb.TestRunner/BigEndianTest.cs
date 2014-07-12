using System;
using System.Diagnostics;

namespace Apaf.NFSdb.TestRunner
{
    public class BigEndianTest : ITask
    {
        public void Run()
        {
            var s = new Stopwatch();
            long sum = 0;
            long sum2 = 0;
            s.Start();
            for (long l = 0; l < 1E9; l++)
            {
                //if (l > 10E6)
                //{
                    var i1 = (int)(l);
                    var s11 = (short) i1;
                    s11 = (short) (((s11 & 0xFF) << 8) | (s11 >> 8) & 0xFF);
                    var s12 = (short) (i1 >> 16);
                    s12 = (short)(((s12 & 0xFF) << 8) | (s12 >> 8) & 0xFF);

                    var i2 = (int)(l >> 32);
                    var s21 = (short) i2;
                    s21 = (short)(((s21 & 0xFF) << 8) | (s21 >> 8) & 0xFF);
                    var s22 = (short)(i2 >> 16);
                    s22 = (short)(((s22 & 0xFF) << 8) | (s22 >> 8) & 0xFF);

                    sum += ((((s11 << 16) | (s12 & 0xFFFF)) & 0xFFFFFFFF) << 32)
                           | ((s21 << 16) | (s22 & 0xFFFF) & 0xFFFFFFFF); 
            }
            s.Stop();
            Console.WriteLine(s.Elapsed);
            Console.WriteLine(sum);
            Console.WriteLine(sum2);
        }

        public string Name
        {
            get { return "big-endian-test"; }
        }

        public static int HostToNetworkOrder(int host)
        {
            return ((HostToNetworkOrder((short)host) & 0xFFFF) << 16)
                    | (HostToNetworkOrder((short)(host >> 16)) & 0xFFFF);
        }

        public static short HostToNetworkOrder(short host)
        {
            return (short)(((host & 0xFF) << 8) | (host >> 8) & 0xFF);
        }
    }
}