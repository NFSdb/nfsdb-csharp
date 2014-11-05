namespace java com.bp.gis.thrift.model
namespace csharp Apaf.NFSdb.TestModel.Model

struct DraughtMinMax {
     1: required string imo;
     2: required double min;
     3: required double max;
     4: required i64 timestamp;
}
