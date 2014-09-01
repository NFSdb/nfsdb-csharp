using Apaf.NFSdb.Core.Storage.Serializer;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Serializer
{
    [TestFixture]
    public class NullableExtensionTests
    {
        [Test]
        public void ShoulChangeNullableValue()
        {
            var nbl = (int?) null;

            var setMethod = NullableExtension.GetSetValue<int>();
        }
    }
}