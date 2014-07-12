namespace java com.bp.gis.thrift.model
namespace csharp Apaf.NJournal.TestModel.Model
	
struct InteractionPoint {
     1: required string id;
     2: required string entityType;
     3: required double lat;
     4: required double lon;
     5: optional string name;
     6: optional bool deleted;
     7: required i64 timestamp;
}
