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

package org.apache.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.thrift.ThriftGenericRow;
import org.apache.druid.data.input.thrift.ThriftParsers;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;

import java.nio.ByteBuffer;
import java.util.List;

public class TwitterThriftInputRowParser implements InputRowParser<Object>
{
  private final ParseSpec parseSpec;
  private final ObjectFlattener<ThriftGenericRow> thriftFlattener;
  private final MapInputRowParser mapParser;

  @JsonCreator
  public TwitterThriftInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;
    this.thriftFlattener = ThriftParsers.makeFlattener(parseSpec);
    this.mapParser = new MapInputRowParser(parseSpec);
  }

  @Override
  public List<InputRow> parseBatch(Object input)
  {
    ThriftGenericRow record;
    if (input instanceof ByteBuffer) { // realtime stream
      record = ThriftParsers.deserialize((ByteBuffer) input);
    } else if (input instanceof ThriftWritable) { // LzoBlockThrift file
      record = (ThriftGenericRow) ((ThriftWritable) input).get();
    } else {
      throw new IAE("unsupport input class of [%s]", input.getClass());
    }

    ThriftParsers.parseThriftGenericRow(record);
    return mapParser.parseBatch(thriftFlattener.flatten(record));
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public InputRowParser<Object> withParseSpec(ParseSpec parseSpec)
  {
    return new TwitterThriftInputRowParser(parseSpec);
  }
}
