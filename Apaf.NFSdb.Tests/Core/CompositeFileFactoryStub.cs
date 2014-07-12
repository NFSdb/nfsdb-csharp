using System;
using System.Collections.Generic;
using System.Linq;
using Apaf.NFSdb.Core.Storage;
using Apaf.NFSdb.Tests.Storage;
using Moq;

namespace Apaf.NFSdb.Tests.Core
{
    public class CompositeFileFactoryStub
    {
        private readonly string _fileNameSize;
        private readonly string _failOnCommitFile;
        private readonly Mock<ICompositeFileFactory> _mock;
        private Dictionary<string, ICompositeFile> _createdFiles;
        private Dictionary<string, IRawFilePart> _filePart;

        public CompositeFileFactoryStub(string fileNameSize,
            string failOnCommitFile = null)
        {
            _fileNameSize = fileNameSize;
            _failOnCommitFile = failOnCommitFile;
            _mock = AppendOffsetStub();
        }

        public Mock<ICompositeFileFactory> Stub
        {
            get { return _mock; }
        }

        public ICompositeFile GetFile(string filename)
        {
            return _createdFiles.First(kv => kv.Key.EndsWith(filename)).Value;
        }

        public IRawFilePart GetFilePart(string filename)
        {
            return _filePart.First(kv => kv.Key.EndsWith(filename)).Value;
        }
        
        private Mock<ICompositeFileFactory> AppendOffsetStub()
        {
            var fileNameParts = _fileNameSize.Split('|');
            var dictSize = new Dictionary<string, long>();
            foreach (var fileSize in fileNameParts)
            {
                var fileSizeArr = fileSize.Trim().Split('-');
                if (fileSizeArr.Length > 1)
                {
                    dictSize[fileSizeArr[0].Trim()] = long.Parse(fileSizeArr[1].Trim());
                }
            }
        
            var stubFileF = new Mock<ICompositeFileFactory>();
            _createdFiles = new Dictionary<string, ICompositeFile>();
            _filePart= new Dictionary<string, IRawFilePart>();
            
            stubFileF.Setup(f => f.OpenFile(It.IsAny<string>(), It.IsAny<int>(),
                It.IsAny<EFileAccess>())).Returns(
                    (string filename, int bithint, EFileAccess access) =>
                    {
                        if (_createdFiles.ContainsKey(filename))
                        {
                            return _createdFiles[filename];
                        }

                        var compFile = new Mock<ICompositeFile>();
                        foreach (var keyVal in dictSize)
                        {
                            if (filename.EndsWith("\\" + keyVal.Key))
                            {
                                IRawFilePart file;
                                if (_failOnCommitFile != null
                                    && filename.EndsWith("\\" + _failOnCommitFile))
                                {
                                    var fileSub = new Mock<IRawFilePart>();
                                    fileSub.Setup(f => f.WriteInt64(0, It.IsAny<long>()))
                                        .Throws(new ArgumentException("stub exception"));
                                    file = fileSub.Object;
                                }
                                else
                                {
                                    file = new BufferBinaryReader(new byte[1024]);
                                    file.WriteInt64(0, keyVal.Value);
                                }
                                _filePart[filename] = file;
                                compFile
                                         .Setup(cf => cf.CreateViewAccessor(0, It.IsAny<long>()))
                                         .Returns(file);

                                _createdFiles[filename] = compFile.Object;
                                return compFile.Object;
                            }
                        }
                        throw new ArgumentOutOfRangeException("No setup for filename " + filename);
                    }
                );

            return stubFileF;
        }
    }
}