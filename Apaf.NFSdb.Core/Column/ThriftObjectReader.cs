//using System;
//using System.Collections.Generic;
//using Apaf.NFSdb.Core.Storage;
//using Thrift.Protocol;

//namespace Apaf.NFSdb.Core.Column
//{
//    public class ThriftObjectReader : IFieldSerializer
//    {
//        private readonly IBitsetColumn _issetColumn;
//        private readonly IList<IColumn> _columns;
//        private readonly Func<object> _constructor;

//        public ThriftObjectReader(IBitsetColumn issetColumn, IList<IColumn> columns, Func<object> constructor)
//        {
//            _issetColumn = issetColumn;
//            _columns = columns;
//            _constructor = constructor;
//        }

//        public void Fill(object item, long rowID, IReadContext readContext)
//        {
//            TProtocol protocol = new NFSdbTProtocol(_issetColumn, _columns, rowID, readContext);
//            ((TBase)item).Read(protocol);
//        }

//        public object Read(long rowID, IReadContext readContext)
//        {
//            var item = _constructor();
//            Fill(item, rowID, readContext);
//            return item;
//        }
//    }
//}