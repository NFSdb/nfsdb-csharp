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
  public partial class DraughtMinMax : TBase
  {
    private string _imo;
    private double _min;
    private double _max;
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

    public double Min
    {
      get
      {
        return _min;
      }
      set
      {
        __isset.min = true;
        this._min = value;
      }
    }

    public double Max
    {
      get
      {
        return _max;
      }
      set
      {
        __isset.max = true;
        this._max = value;
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
      public bool min;
      public bool max;
      public bool timestamp;
    }

    public DraughtMinMax() {
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
            if (field.Type == TType.Double) {
              Min = iprot.ReadDouble();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 3:
            if (field.Type == TType.Double) {
              Max = iprot.ReadDouble();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 4:
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
      TStruct struc = new TStruct("DraughtMinMax");
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
      if (__isset.min) {
        field.Name = "min";
        field.Type = TType.Double;
        field.ID = 2;
        oprot.WriteFieldBegin(field);
        oprot.WriteDouble(Min);
        oprot.WriteFieldEnd();
      }
      if (__isset.max) {
        field.Name = "max";
        field.Type = TType.Double;
        field.ID = 3;
        oprot.WriteFieldBegin(field);
        oprot.WriteDouble(Max);
        oprot.WriteFieldEnd();
      }
      if (__isset.timestamp) {
        field.Name = "timestamp";
        field.Type = TType.I64;
        field.ID = 4;
        oprot.WriteFieldBegin(field);
        oprot.WriteI64(Timestamp);
        oprot.WriteFieldEnd();
      }
      oprot.WriteFieldStop();
      oprot.WriteStructEnd();
    }

    public override string ToString() {
      StringBuilder sb = new StringBuilder("DraughtMinMax(");
      sb.Append("Imo: ");
      sb.Append(Imo);
      sb.Append(",Min: ");
      sb.Append(Min);
      sb.Append(",Max: ");
      sb.Append(Max);
      sb.Append(",Timestamp: ");
      sb.Append(Timestamp);
      sb.Append(")");
      return sb.ToString();
    }

  }

}
