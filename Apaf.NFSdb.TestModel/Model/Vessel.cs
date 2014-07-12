/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Thrift;
using Thrift.Collections;
using Thrift.Protocol;
using Thrift.Transport;
namespace Apaf.NFSdb.TestModel.Model
{

  [Serializable]
  public partial class Vessel : TBase
  {
    private string _imo;
    private string _mmsi;
    private string _name;
    private string _flagCode;
    private string _flagCountry;
    private string _callsign;
    private double _liquidCapacity;
    private double _gasCapacity;
    private long _buildDate;
    private double _deadWeight;
    private double _grossTonnage;
    private double _registeredTonnage;
    private double _length;
    private double _beam;
    private double _maxDraught;
    private string _groupBeneficialOwnerName;
    private string _operatorName;
    private string _complianceCompanyName;
    private string _ownerName;
    private string _shipManagerName;
    private string _hullTypeDesc;
    private string _dwtCategory;
    private string _shipCategory;
    private string _subType;
    private long _timestamp;

    public string Imo
    {
      get
      {
        return _imo;
      }
      set
      {
        __isset.imo = true;
        this._imo = value;
      }
    }

    public string Mmsi
    {
      get
      {
        return _mmsi;
      }
      set
      {
        __isset.mmsi = true;
        this._mmsi = value;
      }
    }

    public string Name
    {
      get
      {
        return _name;
      }
      set
      {
        __isset.name = true;
        this._name = value;
      }
    }

    public string FlagCode
    {
      get
      {
        return _flagCode;
      }
      set
      {
        __isset.flagCode = true;
        this._flagCode = value;
      }
    }

    public string FlagCountry
    {
      get
      {
        return _flagCountry;
      }
      set
      {
        __isset.flagCountry = true;
        this._flagCountry = value;
      }
    }

    public string Callsign
    {
      get
      {
        return _callsign;
      }
      set
      {
        __isset.callsign = true;
        this._callsign = value;
      }
    }

    public double LiquidCapacity
    {
      get
      {
        return _liquidCapacity;
      }
      set
      {
        __isset.liquidCapacity = true;
        this._liquidCapacity = value;
      }
    }

    public double GasCapacity
    {
      get
      {
        return _gasCapacity;
      }
      set
      {
        __isset.gasCapacity = true;
        this._gasCapacity = value;
      }
    }

    public long BuildDate
    {
      get
      {
        return _buildDate;
      }
      set
      {
        __isset.buildDate = true;
        this._buildDate = value;
      }
    }

    public double DeadWeight
    {
      get
      {
        return _deadWeight;
      }
      set
      {
        __isset.deadWeight = true;
        this._deadWeight = value;
      }
    }

    public double GrossTonnage
    {
      get
      {
        return _grossTonnage;
      }
      set
      {
        __isset.grossTonnage = true;
        this._grossTonnage = value;
      }
    }

    public double RegisteredTonnage
    {
      get
      {
        return _registeredTonnage;
      }
      set
      {
        __isset.registeredTonnage = true;
        this._registeredTonnage = value;
      }
    }

    public double Length
    {
      get
      {
        return _length;
      }
      set
      {
        __isset.length = true;
        this._length = value;
      }
    }

    public double Beam
    {
      get
      {
        return _beam;
      }
      set
      {
        __isset.beam = true;
        this._beam = value;
      }
    }

    public double MaxDraught
    {
      get
      {
        return _maxDraught;
      }
      set
      {
        __isset.maxDraught = true;
        this._maxDraught = value;
      }
    }

    public string GroupBeneficialOwnerName
    {
      get
      {
        return _groupBeneficialOwnerName;
      }
      set
      {
        __isset.groupBeneficialOwnerName = true;
        this._groupBeneficialOwnerName = value;
      }
    }

    public string OperatorName
    {
      get
      {
        return _operatorName;
      }
      set
      {
        __isset.operatorName = true;
        this._operatorName = value;
      }
    }

    public string ComplianceCompanyName
    {
      get
      {
        return _complianceCompanyName;
      }
      set
      {
        __isset.complianceCompanyName = true;
        this._complianceCompanyName = value;
      }
    }

    public string OwnerName
    {
      get
      {
        return _ownerName;
      }
      set
      {
        __isset.ownerName = true;
        this._ownerName = value;
      }
    }

    public string ShipManagerName
    {
      get
      {
        return _shipManagerName;
      }
      set
      {
        __isset.shipManagerName = true;
        this._shipManagerName = value;
      }
    }

    public string HullTypeDesc
    {
      get
      {
        return _hullTypeDesc;
      }
      set
      {
        __isset.hullTypeDesc = true;
        this._hullTypeDesc = value;
      }
    }

    public string DwtCategory
    {
      get
      {
        return _dwtCategory;
      }
      set
      {
        __isset.dwtCategory = true;
        this._dwtCategory = value;
      }
    }

    public string ShipCategory
    {
      get
      {
        return _shipCategory;
      }
      set
      {
        __isset.shipCategory = true;
        this._shipCategory = value;
      }
    }

    public string SubType
    {
      get
      {
        return _subType;
      }
      set
      {
        __isset.subType = true;
        this._subType = value;
      }
    }

    public long Timestamp
    {
      get
      {
        return _timestamp;
      }
      set
      {
        __isset.timestamp = true;
        this._timestamp = value;
      }
    }


    public Isset __isset;
    [Serializable]
    public struct Isset {
      public bool imo;
      public bool mmsi;
      public bool name;
      public bool flagCode;
      public bool flagCountry;
      public bool callsign;
      public bool liquidCapacity;
      public bool gasCapacity;
      public bool buildDate;
      public bool deadWeight;
      public bool grossTonnage;
      public bool registeredTonnage;
      public bool length;
      public bool beam;
      public bool maxDraught;
      public bool groupBeneficialOwnerName;
      public bool operatorName;
      public bool complianceCompanyName;
      public bool ownerName;
      public bool shipManagerName;
      public bool hullTypeDesc;
      public bool dwtCategory;
      public bool shipCategory;
      public bool subType;
      public bool timestamp;
    }

    public Vessel() {
    }

    public void Read (TProtocol iprot)
    {
      TField field;
      iprot.ReadStructBegin();
      while (true)
      {
        field = iprot.ReadFieldBegin();
        if (field.Type == TType.Stop) { 
          break;
        }
        switch (field.ID)
        {
          case 1:
            if (field.Type == TType.String) {
              Imo = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 2:
            if (field.Type == TType.String) {
              Mmsi = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 3:
            if (field.Type == TType.String) {
              Name = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 4:
            if (field.Type == TType.String) {
              FlagCode = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 5:
            if (field.Type == TType.String) {
              FlagCountry = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 6:
            if (field.Type == TType.String) {
              Callsign = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 7:
            if (field.Type == TType.Double) {
              LiquidCapacity = iprot.ReadDouble();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 8:
            if (field.Type == TType.Double) {
              GasCapacity = iprot.ReadDouble();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 9:
            if (field.Type == TType.I64) {
              BuildDate = iprot.ReadI64();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 10:
            if (field.Type == TType.Double) {
              DeadWeight = iprot.ReadDouble();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 11:
            if (field.Type == TType.Double) {
              GrossTonnage = iprot.ReadDouble();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 12:
            if (field.Type == TType.Double) {
              RegisteredTonnage = iprot.ReadDouble();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 13:
            if (field.Type == TType.Double) {
              Length = iprot.ReadDouble();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 14:
            if (field.Type == TType.Double) {
              Beam = iprot.ReadDouble();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 15:
            if (field.Type == TType.Double) {
              MaxDraught = iprot.ReadDouble();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 16:
            if (field.Type == TType.String) {
              GroupBeneficialOwnerName = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 17:
            if (field.Type == TType.String) {
              OperatorName = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 18:
            if (field.Type == TType.String) {
              ComplianceCompanyName = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 19:
            if (field.Type == TType.String) {
              OwnerName = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 20:
            if (field.Type == TType.String) {
              ShipManagerName = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 21:
            if (field.Type == TType.String) {
              HullTypeDesc = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 22:
            if (field.Type == TType.String) {
              DwtCategory = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 23:
            if (field.Type == TType.String) {
              ShipCategory = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 24:
            if (field.Type == TType.String) {
              SubType = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 25:
            if (field.Type == TType.I64) {
              Timestamp = iprot.ReadI64();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          default: 
            TProtocolUtil.Skip(iprot, field.Type);
            break;
        }
        iprot.ReadFieldEnd();
      }
      iprot.ReadStructEnd();
    }

    public void Write(TProtocol oprot) {
      TStruct struc = new TStruct("Vessel");
      oprot.WriteStructBegin(struc);
      TField field = new TField();
      if (Imo != null && __isset.imo) {
        field.Name = "imo";
        field.Type = TType.String;
        field.ID = 1;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(Imo);
        oprot.WriteFieldEnd();
      }
      if (Mmsi != null && __isset.mmsi) {
        field.Name = "mmsi";
        field.Type = TType.String;
        field.ID = 2;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(Mmsi);
        oprot.WriteFieldEnd();
      }
      if (Name != null && __isset.name) {
        field.Name = "name";
        field.Type = TType.String;
        field.ID = 3;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(Name);
        oprot.WriteFieldEnd();
      }
      if (FlagCode != null && __isset.flagCode) {
        field.Name = "flagCode";
        field.Type = TType.String;
        field.ID = 4;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(FlagCode);
        oprot.WriteFieldEnd();
      }
      if (FlagCountry != null && __isset.flagCountry) {
        field.Name = "flagCountry";
        field.Type = TType.String;
        field.ID = 5;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(FlagCountry);
        oprot.WriteFieldEnd();
      }
      if (Callsign != null && __isset.callsign) {
        field.Name = "callsign";
        field.Type = TType.String;
        field.ID = 6;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(Callsign);
        oprot.WriteFieldEnd();
      }
      if (__isset.liquidCapacity) {
        field.Name = "liquidCapacity";
        field.Type = TType.Double;
        field.ID = 7;
        oprot.WriteFieldBegin(field);
        oprot.WriteDouble(LiquidCapacity);
        oprot.WriteFieldEnd();
      }
      if (__isset.gasCapacity) {
        field.Name = "gasCapacity";
        field.Type = TType.Double;
        field.ID = 8;
        oprot.WriteFieldBegin(field);
        oprot.WriteDouble(GasCapacity);
        oprot.WriteFieldEnd();
      }
      if (__isset.buildDate) {
        field.Name = "buildDate";
        field.Type = TType.I64;
        field.ID = 9;
        oprot.WriteFieldBegin(field);
        oprot.WriteI64(BuildDate);
        oprot.WriteFieldEnd();
      }
      if (__isset.deadWeight) {
        field.Name = "deadWeight";
        field.Type = TType.Double;
        field.ID = 10;
        oprot.WriteFieldBegin(field);
        oprot.WriteDouble(DeadWeight);
        oprot.WriteFieldEnd();
      }
      if (__isset.grossTonnage) {
        field.Name = "grossTonnage";
        field.Type = TType.Double;
        field.ID = 11;
        oprot.WriteFieldBegin(field);
        oprot.WriteDouble(GrossTonnage);
        oprot.WriteFieldEnd();
      }
      if (__isset.registeredTonnage) {
        field.Name = "registeredTonnage";
        field.Type = TType.Double;
        field.ID = 12;
        oprot.WriteFieldBegin(field);
        oprot.WriteDouble(RegisteredTonnage);
        oprot.WriteFieldEnd();
      }
      if (__isset.length) {
        field.Name = "length";
        field.Type = TType.Double;
        field.ID = 13;
        oprot.WriteFieldBegin(field);
        oprot.WriteDouble(Length);
        oprot.WriteFieldEnd();
      }
      if (__isset.beam) {
        field.Name = "beam";
        field.Type = TType.Double;
        field.ID = 14;
        oprot.WriteFieldBegin(field);
        oprot.WriteDouble(Beam);
        oprot.WriteFieldEnd();
      }
      if (__isset.maxDraught) {
        field.Name = "maxDraught";
        field.Type = TType.Double;
        field.ID = 15;
        oprot.WriteFieldBegin(field);
        oprot.WriteDouble(MaxDraught);
        oprot.WriteFieldEnd();
      }
      if (GroupBeneficialOwnerName != null && __isset.groupBeneficialOwnerName) {
        field.Name = "groupBeneficialOwnerName";
        field.Type = TType.String;
        field.ID = 16;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(GroupBeneficialOwnerName);
        oprot.WriteFieldEnd();
      }
      if (OperatorName != null && __isset.operatorName) {
        field.Name = "operatorName";
        field.Type = TType.String;
        field.ID = 17;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(OperatorName);
        oprot.WriteFieldEnd();
      }
      if (ComplianceCompanyName != null && __isset.complianceCompanyName) {
        field.Name = "complianceCompanyName";
        field.Type = TType.String;
        field.ID = 18;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(ComplianceCompanyName);
        oprot.WriteFieldEnd();
      }
      if (OwnerName != null && __isset.ownerName) {
        field.Name = "ownerName";
        field.Type = TType.String;
        field.ID = 19;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(OwnerName);
        oprot.WriteFieldEnd();
      }
      if (ShipManagerName != null && __isset.shipManagerName) {
        field.Name = "shipManagerName";
        field.Type = TType.String;
        field.ID = 20;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(ShipManagerName);
        oprot.WriteFieldEnd();
      }
      if (HullTypeDesc != null && __isset.hullTypeDesc) {
        field.Name = "hullTypeDesc";
        field.Type = TType.String;
        field.ID = 21;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(HullTypeDesc);
        oprot.WriteFieldEnd();
      }
      if (DwtCategory != null && __isset.dwtCategory) {
        field.Name = "dwtCategory";
        field.Type = TType.String;
        field.ID = 22;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(DwtCategory);
        oprot.WriteFieldEnd();
      }
      if (ShipCategory != null && __isset.shipCategory) {
        field.Name = "shipCategory";
        field.Type = TType.String;
        field.ID = 23;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(ShipCategory);
        oprot.WriteFieldEnd();
      }
      if (SubType != null && __isset.subType) {
        field.Name = "subType";
        field.Type = TType.String;
        field.ID = 24;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(SubType);
        oprot.WriteFieldEnd();
      }
      if (__isset.timestamp) {
        field.Name = "timestamp";
        field.Type = TType.I64;
        field.ID = 25;
        oprot.WriteFieldBegin(field);
        oprot.WriteI64(Timestamp);
        oprot.WriteFieldEnd();
      }
      oprot.WriteFieldStop();
      oprot.WriteStructEnd();
    }

    public override string ToString() {
      StringBuilder sb = new StringBuilder("Vessel(");
      sb.Append("Imo: ");
      sb.Append(Imo);
      sb.Append(",Mmsi: ");
      sb.Append(Mmsi);
      sb.Append(",Name: ");
      sb.Append(Name);
      sb.Append(",FlagCode: ");
      sb.Append(FlagCode);
      sb.Append(",FlagCountry: ");
      sb.Append(FlagCountry);
      sb.Append(",Callsign: ");
      sb.Append(Callsign);
      sb.Append(",LiquidCapacity: ");
      sb.Append(LiquidCapacity);
      sb.Append(",GasCapacity: ");
      sb.Append(GasCapacity);
      sb.Append(",BuildDate: ");
      sb.Append(BuildDate);
      sb.Append(",DeadWeight: ");
      sb.Append(DeadWeight);
      sb.Append(",GrossTonnage: ");
      sb.Append(GrossTonnage);
      sb.Append(",RegisteredTonnage: ");
      sb.Append(RegisteredTonnage);
      sb.Append(",Length: ");
      sb.Append(Length);
      sb.Append(",Beam: ");
      sb.Append(Beam);
      sb.Append(",MaxDraught: ");
      sb.Append(MaxDraught);
      sb.Append(",GroupBeneficialOwnerName: ");
      sb.Append(GroupBeneficialOwnerName);
      sb.Append(",OperatorName: ");
      sb.Append(OperatorName);
      sb.Append(",ComplianceCompanyName: ");
      sb.Append(ComplianceCompanyName);
      sb.Append(",OwnerName: ");
      sb.Append(OwnerName);
      sb.Append(",ShipManagerName: ");
      sb.Append(ShipManagerName);
      sb.Append(",HullTypeDesc: ");
      sb.Append(HullTypeDesc);
      sb.Append(",DwtCategory: ");
      sb.Append(DwtCategory);
      sb.Append(",ShipCategory: ");
      sb.Append(ShipCategory);
      sb.Append(",SubType: ");
      sb.Append(SubType);
      sb.Append(",Timestamp: ");
      sb.Append(Timestamp);
      sb.Append(")");
      return sb.ToString();
    }

  }

}
