using Apaf.NFSdb.Core.Column;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Core.Tx;

namespace Apaf.NFSdb.Tests.Columns.ThriftModel
{

    public interface IColumnStub : IColumn
    {
        object Value { get; }
    }

    public static class ColumnsStub
    {
        public static IColumn CreateColumn<T>(T value, EFieldType fieldType, int fieldID, string propertyName = null)
        {
            return new ColumnStubImpl<T>(value, fieldType, propertyName);
        }

        public class ColumnStubImpl<T> : IFixedWidthColumn, IStringColumn, IColumnStub
        {
            private T _value;

            public ColumnStubImpl(T value, EFieldType ft, string name = null)
            {
                _value = value;
                FieldType = ft;
                PropertyName = name;
            }

            public T1 GetValue<T1>(long rowID)
            {
                return _value != null ? (T1) (object) _value : default(T1);
            }

            public void SetValue<T1>(T1 value)
            {
                _value = (T)(object)value;
            }

            public int GetInt32(long rowID)
            {
                return GetValue<int>(rowID);
            }

            public long GetInt64(long rowID)
            {
                return GetValue<long>(rowID);
            }

            public short GetInt16(long rowID)
            {
                return GetValue<short>(rowID);
            }

            public byte GetByte(long rowID)
            {
                return GetValue<byte>(rowID);
            }

            public bool GetBool(long rowID)
            {
                return GetValue<bool>(rowID);
            }

            public double GetDouble(long rowID)
            {
                return GetValue<double>(rowID);
            }

            public string GetString(long rowID, IReadContext readContext)
            {
                return GetValue<string>(rowID);
            }

            public void SetString(long rowID, string value, ITransactionContext readContext)
            {
                SetValue(value);
            }

            public void SetInt32(long rowID, int value, ITransactionContext readContext)
            {
                SetValue(value);
            }

            public void SetInt64(long rowID, long value, ITransactionContext readContext)
            {
                SetValue(value);
            }

            public void SetInt16(long rowID, short value, ITransactionContext readContext)
            {
                SetValue(value);
            }

            public void SetByte(long rowID, byte value, ITransactionContext readContext)
            {
                SetValue(value);
            }

            public void SetBool(long rowID, bool value, ITransactionContext readContext)
            {
                SetValue(value);
            }

            public void SetDouble(long rowID, double value, ITransactionContext readContext)
            {
                SetValue(value);
            }

            public EFieldType FieldType { get; private set; }
            public string PropertyName { get; private set; }

            public object Value
            {
                get { return _value; }
            }
        }
    }
}