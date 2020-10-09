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

import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.nio.ByteBuffer;

public class ThriftParsers
{
  private static final ThreadLocal<TDeserializer> DESERIALIZER_BINARY = new ThreadLocal<TDeserializer>()
  {
    @Override
    protected TDeserializer initialValue()
    {
      return new TDeserializer(new TBinaryProtocol.Factory());
    }
  };

  public static final String THRIFT_CLASS_NAME = "org.apache.druid.data.input.thrift.ThriftGenericRow";

  private ThriftParsers()
  {
    // No instantiation.
  }

  public static ObjectFlattener<ThriftGenericRow> makeFlattener(
      final ParseSpec parseSpec
  )
  {
    final JSONPathSpec flattenSpec;
    if (parseSpec instanceof ThriftParseSpec) {
      flattenSpec = ((ThriftParseSpec) parseSpec).getFlattenSpec();
    } else {
      flattenSpec = JSONPathSpec.DEFAULT;
    }

    return ObjectFlatteners.create(flattenSpec, new ThriftFlattenerMaker());
  }

  public static ThriftGenericRow deserialize(
      ByteBuffer record
  )
  {
    final byte[] bytes = record.array();
    ThriftGenericRow thriftObj;

    try {
      thriftObj = (ThriftGenericRow) Class.forName(THRIFT_CLASS_NAME).newInstance();
    }
    catch (ClassNotFoundException e) {
      throw new IAE(e, "class [%s] not found in jar", THRIFT_CLASS_NAME);
    }
    catch (InstantiationException | IllegalAccessException e) {
      throw new IAE(e, "instantiation thrift instance failed");
    }

    try {
      DESERIALIZER_BINARY.get().deserialize(thriftObj, bytes);
    }
    catch (TException e) {
      throw new ParseException("this thrift file was not parseable");
    }

    return thriftObj;
  }

  public static void parseThriftGenericRow(
      ThriftGenericRow thriftObj
  )
  {
    try {
      thriftObj.parse();
    }
    catch (TException e) {
      throw new ParseException("this thrift file was not parseable");
    }
  }
}
