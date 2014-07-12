using System;
using System.Linq;
using System.Net;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Storage
{
    [TestFixture]
    public class ByteOrder
    {
        [TestCase(124L)]
        [TestCase(0x10000000000L)]
        public void Int64(long value)
        {
            var rValue = BitConverter.ToInt64(
                BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value)).Reverse().ToArray(), 0);
            Assert.That(rValue, Is.EqualTo(value));
        }
    }
}