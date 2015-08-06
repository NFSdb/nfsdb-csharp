using System;
using System.Collections;
using System.Collections.Generic;

namespace Apaf.NFSdb.Core.Collections
{
    public class ArraySlice<T> : IList<T>
    {
        private readonly T[] _items;
        private readonly int _startIndex;
        private readonly int _length;
        private readonly bool _asc;

        public ArraySlice(T[] items, int startIndex, int length, bool ascending)
        {
            if (startIndex + length > items.Length)
            {
                throw new IndexOutOfRangeException();
            }

            _items = items;
            _startIndex = startIndex;
            _length = startIndex + length;
            _asc = ascending;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return new SliceEnumerator(this, _asc);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(T item)
        {
            throw new NotSupportedException();
        }

        public void Clear()
        {
            throw new NotSupportedException();
        }

        public bool Contains(T item)
        {
            throw new NotSupportedException();
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            if (_asc)
            {
                Array.Copy(_items, _startIndex, array, arrayIndex, _length);
            }
            else
            {
                for (int i = 0; i < Count; i++)
                {
                    array[i + arrayIndex] = _items[_length - 1 - i];
                }
            }
        }

        public bool Remove(T item)
        {
            throw new NotSupportedException();
        }

        public int Count
        {
            get { return _length - _startIndex; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public int IndexOf(T item)
        {
            throw new NotSupportedException();
        }

        public void Insert(int index, T item)
        {
            throw new NotSupportedException();
        }

        public void RemoveAt(int index)
        {
            throw new NotSupportedException();
        }

        public T this[int index]
        {
            get
            {
                if (_startIndex + index > _length || _length - 1 - index < _startIndex)
                {
                    throw new IndexOutOfRangeException();
                }

                return _asc ? _items[_startIndex + index] : _items[_length - 1 - index];
            }
            set
            {
                if (_asc)
                {
                    _items[_startIndex + index] = value; 
                }
                else
                {
                    _items[_length - 1 - index] = value;
                }
            }
        }

        private class SliceEnumerator : IEnumerator<T>
        {
            private readonly ArraySlice<T> _arraySlice;
            private readonly bool _asc;
            private int _index;

            public SliceEnumerator(ArraySlice<T> arraySlice, bool asc)
            {
                _arraySlice = arraySlice;
                _asc = asc;
                _index = asc ? -1 : _arraySlice._length;
            }

            public void Dispose()
            {
            }

            public bool MoveNext()
            {
                _index += _asc ? 1 : -1;
                return _index >= _arraySlice._startIndex && _index < _arraySlice._length;
            }

            public void Reset()
            {
                _index = _asc ? 0 : _arraySlice._items.Length - 1;
            }

            public T Current
            {
                get { return _arraySlice._items[_index]; }
            }

            object IEnumerator.Current
            {
                get { return Current; }
            }
        }
    }
}//I've started to use stuff at work. Have bunch of crash dumps to look at