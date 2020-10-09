/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.data.input.thrift;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ThriftGenericRow
    implements TBase<ThriftGenericRow, ThriftGenericRow.Fields>
{
  public static final Logger log = LoggerFactory.getLogger(ThriftGenericRow.class);
  private final Map<String, Fields> topLevelSchema = new HashMap<>();
  private final Map<String, Integer> fieldOffsets = new HashMap<>();
  private final Set<String> fieldDeserialized = new HashSet<>();
  private final Map<String, Object> fieldValues = new HashMap<>();
  private byte[] buf;
  private int off;
  private int len;

  public ThriftGenericRow()
  {
  }

  public ThriftGenericRow(
      Map<String, Integer> fieldOffsets,
      Set<String> fieldDeserialized,
      Map<String, Object> fieldValues
  )
  {
    this.fieldOffsets.putAll(fieldOffsets);
    this.fieldDeserialized.addAll(fieldDeserialized);
    this.fieldValues.putAll(fieldValues);
  }

  public Map<String, Fields> getTopLevelSchema()
  {
    return topLevelSchema;
  }

  public static class Fields
      implements TFieldIdEnum
  {
    private final short thriftId;
    private final String fieldName;
    private final byte fieldType;
    private final int fieldOffset;
    private byte[] buf;
    private int len;

    Fields(short thriftId, String fieldName, byte type, int fieldOffset, byte[] buf, int len)
    {
      this.thriftId = thriftId;
      this.fieldName = fieldName;
      this.fieldType = type;
      this.fieldOffset = fieldOffset;
      this.buf = buf;
      this.len = len;
    }

    Fields(short thriftId, String fieldName)
    {
      this.thriftId = thriftId;
      this.fieldName = fieldName;
      this.fieldType = TType.VOID;
      this.fieldOffset = 0;
    }

    @Override
    public short getThriftFieldId()
    {
      return thriftId;
    }

    @Override
    public String getFieldName()
    {
      return fieldName;
    }

    public byte getFieldType()
    {
      return fieldType;
    }

    public byte getElementFieldType()
    {
      TMemoryInputTransport trans = new TMemoryInputTransport(buf, fieldOffset, len);
      TBinaryProtocol iprot = new TBinaryProtocol(trans);
      byte elementType = TType.STOP;

      try {
        if (fieldType == TType.LIST) {
          TList ilist = iprot.readListBegin();
          elementType = ilist.elemType;
          iprot.readListEnd();
        } else if (fieldType == TType.SET) {
          TSet iSet = iprot.readSetBegin();
          elementType = iSet.elemType;
          iprot.readSetEnd();
        }
      }
      catch (TException e) {

      }

      return elementType;
    }
  }

  @Override
  public void read(TProtocol iprot)
      throws TException
  {
    TTransport trans = iprot.getTransport();
    buf = trans.getBuffer();
    off = trans.getBufferPosition();
    TProtocolUtil.skip(iprot, TType.STRUCT);
    len = trans.getBufferPosition() - off;
  }

  public void parse()
      throws TException
  {
    TMemoryInputTransport trans = new TMemoryInputTransport(buf, off, len);
    TBinaryProtocol iprot = new TBinaryProtocol(trans);
    TField field;
    iprot.readStructBegin();
    while (true) {
      int fieldOffset = trans.getBufferPosition();
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) {
        break;
      }

      topLevelSchema.put("" + field.id, new Fields(field.id, "" + field.id, field.type, fieldOffset, buf, len));
      fieldOffsets.put("" + field.id, fieldOffset);
      TProtocolUtil.skip(iprot, field.type);
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
  }

  private Object readElem(TProtocol iprot, byte type)
      throws TException
  {
    switch (type) {
      case TType.BOOL:
        return iprot.readBool();
      case TType.BYTE:
        return iprot.readByte();
      case TType.I16:
        return iprot.readI16();
      case TType.ENUM:
      case TType.I32:
        return iprot.readI32();
      case TType.I64:
        return iprot.readI64();
      case TType.DOUBLE:
        return iprot.readDouble();
      case TType.STRING:
        return iprot.readString();
      case TType.STRUCT:
        return readStruct(iprot);
      case TType.LIST:
        return readList(iprot);
      case TType.SET:
        return readSet(iprot);
      case TType.MAP:
        return readMap(iprot);
      default:
        TProtocolUtil.skip(iprot, type);
        return null;
    }
  }

  private Object readStruct(TProtocol iprot)
      throws TException
  {
    ThriftGenericRow elem = new ThriftGenericRow();
    elem.read(iprot);
    elem.parse();
    return elem;
  }

  private Object readList(TProtocol iprot)
      throws TException
  {
    TList ilist = iprot.readListBegin();
    List<Object> listValue = new ArrayList<>();
    for (int i = 0; i < ilist.size; i++) {
      listValue.add(readElem(iprot, ilist.elemType));
    }
    iprot.readListEnd();
    return listValue;
  }

  private Object readSet(TProtocol iprot)
      throws TException
  {
    TSet iset = iprot.readSetBegin();
    List<Object> setValue = new ArrayList<>();
    for (int i = 0; i < iset.size; i++) {
      setValue.add(readElem(iprot, iset.elemType));
    }
    iprot.readSetEnd();
    return setValue;
  }

  private Object readMap(TProtocol iprot)
      throws TException
  {
    TMap imap = iprot.readMapBegin();
    Map<Object, Object> mapValue = new HashMap<>();
    for (int i = 0; i < imap.size; i++) {
      mapValue.put(readElem(iprot, imap.keyType), readElem(iprot, imap.valueType));
    }
    iprot.readMapEnd();
    return mapValue;
  }

  public Object getFieldValueForThriftId(String thriftId)
  {
    if (!fieldDeserialized.contains(thriftId) && fieldOffsets.containsKey("" + thriftId)) {
      try {
        TMemoryInputTransport trans = new TMemoryInputTransport(buf, fieldOffsets.get("" + thriftId), len);
        TBinaryProtocol iprot = new TBinaryProtocol(trans);

        TField field = iprot.readFieldBegin();
        fieldValues.put("" + field.id, readElem(iprot, field.type));
        fieldDeserialized.add("" + field.id);
        iprot.readFieldEnd();
      }
      catch (TException e) {
        log.error("Failed to decode the field with thrift id: " + thriftId + ", the field may be returned as null", e);
      }
    }

    if (fieldValues.containsKey(thriftId)) {
      return fieldValues.get(thriftId);
    }

    return null;
  }

  @Override
  public ThriftGenericRow deepCopy()
  {
    return new ThriftGenericRow(fieldOffsets, fieldDeserialized, fieldValues);
  }

  @Override
  public void clear()
  {

  }

  public int length()
  {
    return len;
  }

  @Override
  public Fields fieldForId(int fieldId)
  {
    return new Fields((short) fieldId, "" + fieldId);
  }

  @Override
  public Object getFieldValue(Fields field)
  {
    return getFieldValueForThriftId("" + field.getThriftFieldId());
  }

  public Set<String> getFieldIds()
  {
    return fieldOffsets.keySet();
  }

  @Override
  public boolean isSet(Fields field)
  {
    return fieldOffsets.containsKey("" + field.getThriftFieldId());
  }

  @Override
  public void setFieldValue(Fields field, Object value)
  {
    throw new UnsupportedOperationException("ThriftGenericRow.setFieldValue is not supported.");
  }

  @Override
  public void write(TProtocol oprot)
  {
    throw new UnsupportedOperationException("ThriftGenericRow.write is not supported.");
  }

  @Override
  public int compareTo(ThriftGenericRow other)
  {
    throw new UnsupportedOperationException("ThriftGenericRow.compareTo is not supported.");
  }
}
