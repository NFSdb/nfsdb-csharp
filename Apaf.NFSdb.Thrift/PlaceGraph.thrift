namespace java com.bp.gis.thrift.model
namespace csharp Apaf.NFSdb.TestModel.Model

struct PlaceGraph {
     1: required string subj;
     2: required string subjType;
     3: required string predicate;
     4: required string obj;
     5: required string objType;
     6: required i64 timestamp;
     7: optional bool deleted;
}
