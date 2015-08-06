using System;
using System.Collections.Generic;

namespace Apaf.NFSdb.Core.Collections
{
    public class PriorityQueue<T>
    {
        private T[] _items;
        private int _count;
        private readonly IComparer<T> _comparer;

        public PriorityQueue(int capacity)
        {
            _items = new T[capacity];
        }

        public PriorityQueue() : this(1)
        {
        }

        public PriorityQueue(int initCapacity, IComparer<T> comparer)
            : this(initCapacity)
        {
            _comparer = comparer;
        }


        public PriorityQueue(IComparer<T> comparer)
            : this()
        {
            _comparer = comparer;
        }

        public int Count
        {
            get { return _count; }
        }

        public void Enqueue(T item)
        {
            if (_count + 1 > _items.Length)
            {
                Resize(checked (_items.Length * 2));
            }
            _items[_count++] = item;
            Swim(_count-1);
        }

        public T Dequeue()
        {
            if (_count == 0)
            {
                throw new InvalidOperationException("PriorityQueue is empty");
            }
            var max = _items[0];
            Swap(0, --_count);
            Sink(0);
            _items[_count] = default(T);
            return max;
        }

        protected virtual int Compare(T i1, T i2)
        {
            if (_comparer != null)
            {
                return _comparer.Compare(i1, i2);
            }
            return ((IComparable<T>) i1).CompareTo(i2);
        }

        private void Resize(int newCapacity)
        {
            var newItems = new T[newCapacity];
            Array.Copy(_items, 0, newItems, 0, _count);
            _items = newItems;
        }

        private void Swim(int i)
        {
            while (i > 0 && Compare(_items[(i - 1)/2], _items[i]) > 0)
            {
                Swap(i, (i - 1) / 2);
                i = (i - 1) / 2;
            }
        }

        private void Sink(int i)
        {
            while (2*i + 1 < _count)
            {
                int j = 2*i + 1;
                if (j < _count - 1 && Compare(_items[j], _items[j + 1]) > 0)
                {
                    j++;
                }
                if (Compare(_items[i], _items[j]) <= 0)
                {
                    break;
                }
                Swap(i, j);
                i = j;
            }
        }

        private void Swap(int i1, int i2)
        {
            T t = _items[i1];
            _items[i1] = _items[i2];
            _items[i2] = t;
        }
    }
}