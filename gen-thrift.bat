thrift-0.8.0 --gen csharp "thrift\Draught.thrift"
thrift-0.8.0 --gen csharp "thrift\InteractionPoint.thrift"	
thrift-0.8.0 --gen csharp "thrift\PlaceGraph.thrift"
thrift-0.8.0 --gen csharp "thrift\Quote.thrift"
thrift-0.8.0 --gen csharp "thrift\ShipTracker.thrift"
thrift-0.8.0 --gen csharp "thrift\Trade.thrift"
thrift-0.8.0 --gen csharp "thrift\Vessel.thrift"
thrift-0.8.0 --gen csharp "thrift\VesselPosition.thrift"
thrift-0.8.0 --gen csharp "thrift\VesselVisit.thrift"
xcopy gen-csharp\Apaf\NJournal\TestModel\Model\*.* Apaf.NJournal.TestModel\Model
