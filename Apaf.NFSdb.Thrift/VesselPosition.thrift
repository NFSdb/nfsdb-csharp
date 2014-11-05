namespace java com.bp.gis.thrift.model
namespace csharp Apaf.NFSdb.TestModel.Model

struct VesselPosition {
     1: required string imo;
     2: optional string name;
     3: required double lat;
     4: required double lon;
     5: required double angle;
     6: optional double speed;
     7: optional string status;
     8: optional double draught;
     9: optional string dest;
    10: required i64 timestamp;
    11: optional i32 stationaryMinutes;
    12: required string feed;
    13: optional i64 eta
    14: optional double quality;
}
