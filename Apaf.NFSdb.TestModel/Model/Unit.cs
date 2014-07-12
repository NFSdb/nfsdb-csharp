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
  public partial class Unit : TBase
  {
    private string _unitID;
    private string _unitName;
    private string _unitType;
    private string _subUnitType;
    private double _capacity;
    private long _timestamp;

    public string UnitID
    {
      get
      {
        return _unitID;
      }
      set
      {
        __isset.unitID = true;
        this._unitID = value;
      }
    }

    public string UnitName
    {
      get
      {
        return _unitName;
      }
      set
      {
        __isset.unitName = true;
        this._unitName = value;
      }
    }

    public string UnitType
    {
      get
      {
        return _unitType;
      }
      set
      {
        __isset.unitType = true;
        this._unitType = value;
      }
    }

    public string SubUnitType
    {
      get
      {
        return _subUnitType;
      }
      set
      {
        __isset.subUnitType = true;
        this._subUnitType = value;
      }
    }

    public double Capacity
    {
      get
      {
        return _capacity;
      }
      set
      {
        __isset.capacity = true;
        this._capacity = value;
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
      public bool unitID;
      public bool unitName;
      public bool unitType;
      public bool subUnitType;
      public bool capacity;
      public bool timestamp;
    }

    public Unit() {
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
          case 2:
            if (field.Type == TType.String) {
              UnitID = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 3:
            if (field.Type == TType.String) {
              UnitName = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 4:
            if (field.Type == TType.String) {
              UnitType = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 5:
            if (field.Type == TType.String) {
              SubUnitType = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 6:
            if (field.Type == TType.Double) {
              Capacity = iprot.ReadDouble();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 7:
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
      TStruct struc = new TStruct("Unit");
      oprot.WriteStructBegin(struc);
      TField field = new TField();
      if (UnitID != null && __isset.unitID) {
        field.Name = "unitID";
        field.Type = TType.String;
        field.ID = 2;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(UnitID);
        oprot.WriteFieldEnd();
      }
      if (UnitName != null && __isset.unitName) {
        field.Name = "unitName";
        field.Type = TType.String;
        field.ID = 3;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(UnitName);
        oprot.WriteFieldEnd();
      }
      if (UnitType != null && __isset.unitType) {
        field.Name = "unitType";
        field.Type = TType.String;
        field.ID = 4;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(UnitType);
        oprot.WriteFieldEnd();
      }
      if (SubUnitType != null && __isset.subUnitType) {
        field.Name = "subUnitType";
        field.Type = TType.String;
        field.ID = 5;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(SubUnitType);
        oprot.WriteFieldEnd();
      }
      if (__isset.capacity) {
        field.Name = "capacity";
        field.Type = TType.Double;
        field.ID = 6;
        oprot.WriteFieldBegin(field);
        oprot.WriteDouble(Capacity);
        oprot.WriteFieldEnd();
      }
      if (__isset.timestamp) {
        field.Name = "timestamp";
        field.Type = TType.I64;
        field.ID = 7;
        oprot.WriteFieldBegin(field);
        oprot.WriteI64(Timestamp);
        oprot.WriteFieldEnd();
      }
      oprot.WriteFieldStop();
      oprot.WriteStructEnd();
    }

    public override string ToString() {
      StringBuilder sb = new StringBuilder("Unit(");
      sb.Append("UnitID: ");
      sb.Append(UnitID);
      sb.Append(",UnitName: ");
      sb.Append(UnitName);
      sb.Append(",UnitType: ");
      sb.Append(UnitType);
      sb.Append(",SubUnitType: ");
      sb.Append(SubUnitType);
      sb.Append(",Capacity: ");
      sb.Append(Capacity);
      sb.Append(",Timestamp: ");
      sb.Append(Timestamp);
      sb.Append(")");
      return sb.ToString();
    }

  }

}
