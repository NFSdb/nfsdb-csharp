namespace java com.bp.gis.thrift.model
namespace csharp Apaf.NJournal.TestModel.Model

struct Vessel {
     1: optional string imo;
     2: optional string mmsi;
     3: optional string name;
     4: optional string flagCode;
     5: optional string flagCountry;
     6: optional string callsign;
     7: optional double liquidCapacity;
     8: optional double gasCapacity;
     9: optional i64 buildDate;
    10: optional double deadWeight;
    11: optional double grossTonnage;
    12: optional double registeredTonnage;
    13: optional double length;
    14: optional double beam;
    15: optional double maxDraught;
    16: optional string groupBeneficialOwnerName;
    17: optional string operatorName;
    18: optional string complianceCompanyName;
    19: optional string ownerName;
    20: optional string shipManagerName;
    21: optional string hullTypeDesc;
    22: optional string dwtCategory;
    23: optional string shipCategory;
    24: optional string subType;
    25: optional i64 timestamp;
}
