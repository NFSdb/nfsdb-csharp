namespace java com.bp.gis.thrift.model
namespace csharp Apaf.NJournal.TestModel.Model

// Deprecated port visit domain class.
struct VesselPortVisit {
     1: required string imo;
     2: required string port;
     3: required i64 startTimestamp;
     4: required i64 endTimestamp;
     5: optional double draught;
}
