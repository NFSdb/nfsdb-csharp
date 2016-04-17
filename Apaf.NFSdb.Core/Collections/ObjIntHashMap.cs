using System;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Collections
{
    public class ObjIntHashMap : IObjIntHashMap
    {
        private const int NO_KEY_VALUE = MetadataConstants.SYMBOL_NOT_FOUND_VALUE;
        private readonly double _loadFactor;
        private string[] _keys;
        private int[] _values;
        private int[] _valuesMap;
        private int _free;
        private int _capacity;
        private int _nullKey = NO_KEY_VALUE;

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
            _valuesMap = new int[capacity];
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
            _valuesMap = new int[newCapacity];
            _valuesMap[0] = NO_KEY_VALUE;

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
            if (key == null)
            {
                return _nullKey;
            }
            int index = (HashCode(key) & 0x7fffffff) % _keys.Length;

            string val = _keys[index];
            if (val == null)
            {
                return NO_KEY_VALUE;
            }

            if (ReferenceEquals(val, key) || key == val)
            {
                return _values[index];
            }

            return Probe(key, index);
        }

       private static unsafe int HashCode(string key)
       {
           int h = 0;
           var len = key.Length;
           if (len > 0)
           {
               fixed (char* buff = key)
               {
                   for (int i = 0; i < len; i++)
                   {
                       h = 31*h + buff[i];
                   }
               }
           }
           return h;
       }

        private int Probe(string key, int index)
        {
            do
            {
                index = (index + 1)%_keys.Length;
                if (_keys[index] == null)
                {
                    return NO_KEY_VALUE;
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
                var exists = _nullKey == NO_KEY_VALUE;
                if (!exists) _nullKey = value;
                return exists;
            }

            int index = (HashCode(key) & 0x7fffffff)%_keys.Length;
            if (_keys[index] == null)
            {
                _keys[index] = key;
                _values[index] = value;
                if (value >= 0 && value < _valuesMap.Length)
                {
                    _valuesMap[value] = index;
                }
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
            int index = (HashCode(key) & 0x7fffffff) % _keys.Length;
            if (_keys[index] == null)
            {
                _keys[index] = key;
                _values[index] = value;
                if (value >= 0 && value < _valuesMap.Length)
                {
                    _valuesMap[value] = index;
                }
                _free--;
                if (_free == 0)
                {
                    Rehash();
                }
                return NO_KEY_VALUE;
            }

            if (ReferenceEquals(_keys[index], key) || key.Equals(_keys[index]))
            {
                int old = _values[index];
                _values[index] = value;
                if (value >= 0 && value < _valuesMap.Length)
                {
                    _valuesMap[value] = index;
                }
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
                    if (value >= 0 && value < _valuesMap.Length)
                    {
                        _valuesMap[value] = index;
                    }
                    _free--;
                    if (_free == 0)
                    {
                        Rehash();
                    }
                    return NO_KEY_VALUE;
                }

                if (ReferenceEquals(_keys[index], key) || key.Equals(_keys[index]))
                {
                    int old = _values[index];
                    _values[index] = value;
                    if (value >= 0 && value < _valuesMap.Length)
                    {
                        _valuesMap[value] = index;
                    }
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
                    if (value >= 0 && value < _valuesMap.Length)
                    {
                        _valuesMap[value] = index;
                    }
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

        public bool LookupValue(int key, out string value)
        {
            try
            {
                var values = _valuesMap[key];
                value = _keys[values];
                return Get(value) == key;
            }
            catch (IndexOutOfRangeException)
            {
            }
            value = null;
            return false;
        }
    }
}