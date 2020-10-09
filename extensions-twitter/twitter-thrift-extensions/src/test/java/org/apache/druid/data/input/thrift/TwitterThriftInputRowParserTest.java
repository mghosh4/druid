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
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.TwitterThriftInputRowParser;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

public class TwitterThriftInputRowParserTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private ParseSpec parseSpec;

  private TwitterThriftInputRowParser parser;

  private Book book;

  @Before
  public void setUp()
  {
    book = new Book().setPrice(19.9).setTitle("title")
                          .setAuthor(new Author().setFirstName("first").setLastName("last").setDate("2016-08-29"));
    parseSpec = new ThriftParseSpec(new TimestampSpec("ts", "auto", null),
                                  new DimensionsSpec(Lists.newArrayList(
                                      new StringDimensionSchema("title"),
                                      new StringDimensionSchema("lastName")
                                  ), null, null),
                                  new JSONPathSpec(
                                      true,
                                      Lists.newArrayList(
                                          new JSONPathFieldSpec(JSONPathFieldType.PATH, "ts", "$.4.3"),
                                          new JSONPathFieldSpec(JSONPathFieldType.ROOT, "title", "3"),
                                          new JSONPathFieldSpec(JSONPathFieldType.PATH, "lastName", "$.4.2")
                                      )
                                  )
    );

    parser = new TwitterThriftInputRowParser(
        parseSpec
    );
  }

  @Test
  public void testParseStream() throws Exception
  {
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    serializationAndTest(parser, ByteBuffer.wrap(serializer.serialize(book)));
  }

  @Test
  public void testParseHadoop() throws Exception
  {
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    serializationAndTest(parser, new ThriftWritable(ThriftParsers.deserialize(ByteBuffer.wrap(serializer.serialize(book))),
        new TypeRef<ThriftGenericRow>()
        {
          @Override
          public ThriftGenericRow newInstance()
              throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
          {
            return super.newInstance();
          }
        }));
  }

  private void serializationAndTest(TwitterThriftInputRowParser parser, Object row)
  {
    InputRow row1 = parser.parseBatch(row).get(0);
    Assert.assertEquals("title", row1.getDimension("title").get(0));
    Assert.assertEquals("last", row1.getDimension("lastName").get(0));
  }
}
