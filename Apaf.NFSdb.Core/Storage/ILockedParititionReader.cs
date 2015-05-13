using System;

namespace Apaf.NFSdb.Core.Storage
{
    public interface ILockedParititionReader : IPartitionReader, IDisposable
    {
    }
}