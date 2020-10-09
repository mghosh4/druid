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

import com.google.common.collect.Lists;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JsonProvider;
import org.apache.druid.java.util.common.parsers.NotImplementedMappingProvider;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.thrift.protocol.TType;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ThriftFlattenerMaker implements ObjectFlatteners.FlattenerMaker<ThriftGenericRow>
{
  private static final JsonProvider THRIFT_JSON_PROVIDER = new GenericThriftJsonProvider();
  private static final Configuration JSONPATH_CONFIGURATION =
      Configuration.builder()
                   .jsonProvider(THRIFT_JSON_PROVIDER)
                   .mappingProvider(new NotImplementedMappingProvider())
                   .options(EnumSet.of(Option.SUPPRESS_EXCEPTIONS))
                   .build();

  private static final List<Byte> PRIMITIVE_TYPES = Lists.newArrayList(
      TType.STRING,
      TType.BYTE,
      TType.I16,
      TType.I32,
      TType.I64,
      TType.BOOL,
      TType.DOUBLE
  );

  private static boolean isPrimitive(byte type)
  {
    return PRIMITIVE_TYPES.contains(type);
  }

  private static boolean isPrimitiveArray(byte type)
  {
    return type == TType.LIST || type == TType.SET;
  }

  private static boolean isFieldPrimitive(ThriftGenericRow.Fields field)
  {
    return isPrimitive(field.getFieldType()) ||
           (isPrimitiveArray(field.getFieldType()) && isPrimitive(field.getElementFieldType()));
  }

  public ThriftFlattenerMaker()
  {
  }

  @Override
  public Set<String> discoverRootFields(final ThriftGenericRow obj)
  {
    return obj.getTopLevelSchema()
              .values()
              .stream()
              .filter(ThriftFlattenerMaker::isFieldPrimitive)
              .map(ThriftGenericRow.Fields::getFieldName)
              .collect(Collectors.toSet());
  }

  @Override
  public Object getRootField(final ThriftGenericRow record, final String key)
  {
    return transformValue(record.getFieldValueForThriftId(key));
  }

  @Override
  public Function<ThriftGenericRow, Object> makeJsonPathExtractor(final String expr)
  {
    final JsonPath jsonPath = JsonPath.compile(expr);
    return record -> transformValue(jsonPath.read(record, JSONPATH_CONFIGURATION));
  }

  @Override
  public Function<ThriftGenericRow, Object> makeJsonQueryExtractor(final String expr)
  {
    throw new UnsupportedOperationException("Avro + JQ not supported");
  }

  @Override
  public JsonProvider getJsonProvider()
  {
    return THRIFT_JSON_PROVIDER;
  }

  private Object transformValue(final Object field)
  {
    if (field instanceof ByteBuffer) {
      return ((ByteBuffer) field).array();
    } else if (field instanceof List) {
      return ((List<?>) field).stream().filter(Objects::nonNull).collect(Collectors.toList());
    }
    return field;
  }
}
