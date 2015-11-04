thrift-0.8.0 --gen csharp "Apaf.NFSdb.Thrift\Quote.thrift"
thrift-0.8.0 --gen csharp "Apaf.NFSdb.Thrift\Trade.thrift"
xcopy /y gen-csharp\Apaf\NFSdb\TestShared\Model\*.* Apaf.NFSdb.TestShared\Model 
