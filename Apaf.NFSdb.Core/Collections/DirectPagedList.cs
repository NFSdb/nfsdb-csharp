using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Apaf.NFSdb.Core.Collections
{
    public class DirectPagedList<T> where T : struct
    {
        private long _length;
        private List<IntPtr> _pages;
        private readonly int _pageBitHint;

        public DirectPagedList(int pageBlock = 64*1024)
        {
            _pageBitHint = (int)Math.Ceiling(Math.Log(
                Math.Max(pageBlock / Marshal.SizeOf(typeof(T)), 1), 2));

            _pages = new List<IntPtr>();
        }

        public void Add(T item)
        {
            var index = _length + 1;
        }
    }
}