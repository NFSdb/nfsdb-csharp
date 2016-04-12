using System;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Collections
{
    public class ObjIntHashMap : IObjIntHashMap
    {
        private const int NoKeyValue = MetadataConstants.SYMBOL_NOT_FOUND_VALUE;
        private readonly double _loadFactor;
        private string[] _keys;
        private int[] _values;
        private int _free;
        private int _capacity;
        private int _nullKey = NoKeyValue;

        public ObjIntHashMap() : this(11)
        {
        }

        public ObjIntHashMap(int initialCapacity) : this(initialCapacity, 0.5)
        {
        }
        
        public ObjIntHashMap(int initialCapacity, double loadFactor)
        {
            int capacity = Math.Max(initialCapacity, (int) (initialCapacity/loadFactor));
            _loadFactor = loadFactor;
            _keys = new string[capacity];
            _values = new int[capacity];
            _free = _capacity = initialCapacity;
        }

        protected void Rehash()
        {
            int newCapacity = Primes.Next(_values.Length << 1);
            _free = _capacity = (int) (newCapacity*_loadFactor);
            int[] oldValues = _values;
            string[] oldKeys = _keys;
            _keys = new string[newCapacity];
            _values = new int[newCapacity];
            for (int i = 0; i < _keys.Length; i++)
            {
                _keys[i] = null;
            }

            for (int i = oldKeys.Length; i-- > 0;)
            {
                if (oldKeys[i] != null)
                {
                    InsertKey(oldKeys[i], oldValues[i]);
                }
            }
        }

        public int Get(string key)
        {
            if (key == null) return _nullKey;

            int index = (key.GetHashCode() & 0x7fffffff) % _keys.Length;

            if (_keys[index] == null)
            {
                return NoKeyValue;
            }

            if (ReferenceEquals(_keys[index], key) || key == _keys[index])
            {
                return _values[index];
            }

            return Probe(key, index);
        }

        private int Probe(string key, int index)
        {
            do
            {
                index = (index + 1)%_keys.Length;
                if (_keys[index] == null)
                {
                    return NoKeyValue;
                }
                if (ReferenceEquals(_keys[index], key) || key.Equals(_keys[index]))
                {
                    return _values[index];
                }
            } while (true);
        }

        public int Put(string key, int value)
        {
            if (key != null)
            {
                return InsertKey(key, value);
            }

            return PutNull(value);
        }

        private int PutNull(int value)
        {
            var old = _nullKey;
            _nullKey = value;
            return old;
        }

        public bool PutIfAbsent(string key, int value)
        {
            if (key == null)
            {
                var exists = _nullKey == NoKeyValue;
                if (!exists) _nullKey = value;
                return exists;
            }

            int index = (key.GetHashCode() & 0x7fffffff)%_keys.Length;
            if (_keys[index] == null)
            {
                _keys[index] = key;
                _values[index] = value;
                _free--;
                if (_free == 0)
                {
                    Rehash();
                }
                return true;
            }

            return !(_keys[index] == key || key.Equals(_keys[index])) &&
                   ProbeInsertIfAbsent(key, index, value);
        }

        private int InsertKey(string key, int value)
        {
            int index = (key.GetHashCode() & 0x7fffffff)%_keys.Length;
            if (_keys[index] == null)
            {
                _keys[index] = key;
                _values[index] = value;
                _free--;
                if (_free == 0)
                {
                    Rehash();
                }
                return NoKeyValue;
            }

            if (ReferenceEquals(_keys[index], key) || key.Equals(_keys[index]))
            {
                int old = _values[index];
                _values[index] = value;
                return old;
            }

            return ProbeInsert(key, index, value);
        }

        private int ProbeInsert(string key, int index, int value)
        {
            do
            {
                index = (index + 1)%_keys.Length;
                if (_keys[index] == null)
                {
                    _keys[index] = key;
                    _values[index] = value;
                    _free--;
                    if (_free == 0)
                    {
                        Rehash();
                    }
                    return NoKeyValue;
                }

                if (ReferenceEquals(_keys[index], key) || key.Equals(_keys[index]))
                {
                    int old = _values[index];
                    _values[index] = value;
                    return old;
                }
            } while (true);
        }

        private bool ProbeInsertIfAbsent(string key, int index, int value)
        {
            do
            {
                index = (index + 1)%_keys.Length;
                if (_keys[index] == null)
                {
                    _keys[index] = key;
                    _values[index] = value;
                    _free--;
                    if (_free == 0)
                    {
                        Rehash();
                    }
                    return true;
                }

                if (_keys[index] == key || key.Equals(_keys[index]))
                {
                    return false;
                }
            } while (true);
        }

        public void Clear()
        {
            for (int i = 0; i < _keys.Length; i++)
            {
                _keys[i] = null;
            }
        }

        public int Size()
        {
            return _capacity - _free;
        }
    }
}