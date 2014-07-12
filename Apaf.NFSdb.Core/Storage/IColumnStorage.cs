using System.Collections.Generic;

namespace Apaf.NFSdb.Core.Storage
{
    public interface IColumnStorage
    {
        IRawFile GetFile(string fieldName, int fileID, int columnID, EDataType dataType);
        IEnumerable<IRawFile> AllOpenedFiles();
    }
}