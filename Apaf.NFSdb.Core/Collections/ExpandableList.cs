﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Apaf.NFSdb.Core.Collections
{
    public class ExpandableList<T> : ICollection<T>
    {
        private readonly Func<T> _createNew;
        private readonly List<T> _list = new List<T>();

        public ExpandableList()
        {
        }

        public ExpandableList(Func<T> createNew)
        {
            _createNew = createNew;
        }

        public ExpandableList(IEnumerable<T> copyFrom, Func<T> createNew = null) : 
            this(createNew)
        {
            _list = new List<T>(copyFrom);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(T item)
        {
            _list.Add(item);
        }

        public void Clear()
        {
            _list.Clear();
        }

        public bool Contains(T item)
        {
            return _list.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            _list.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            return _list.Remove(item);
        }

        public int Count { get { return _list.Count; } }
        public bool IsReadOnly { get { return false; } }

        public T this[int index]
        {
            get
            {
                if (index >= _list.Count)
                {
                    _list.AddRange(Enumerable.Repeat(CreateItem(),
                        index + 1 - _list.Count));
                }
                return _list[index];
            }
            set
            {
                if (index >= _list.Count)
                {
                    _list.AddRange(Enumerable.Repeat(CreateItem(),
                        index + 1 - _list.Count));
                }
                _list[index] = value;
            }
        }

        private T CreateItem()
        {
            if (_createNew != null)
            {
                return _createNew();
            }
            return default(T);
        }
    }
}