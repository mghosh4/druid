/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.extraction;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class LookupDelegatorTest
{
  private final LookupReferencesManager lookupReferencesManager = EasyMock.createMock(LookupReferencesManager.class);
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private LookupExtractor lookupExtractor;
  private final LookupExtractorFactory mockLookupFactory = EasyMock.createMock(LookupExtractorFactory.class);
  private final LookupExtractor mockLookup = EasyMock.createMock(LookupExtractor.class);
  private final String name = "name";

  @Test
  public void testSerDeser() throws IOException
  {
    String serObject = mapper.writeValueAsString(lookupExtractor);
    Assert.assertEquals(lookupExtractor, mapper.reader(LookupExtractor.class).readValue(serObject));
  }

  @Before
  public void setUp()
  {
    EasyMock.expect(lookupReferencesManager.get(name))
            .andReturn(mockLookupFactory)
            .anyTimes();
    EasyMock.expect(mockLookupFactory.start()).andReturn(true).anyTimes();
    EasyMock.expect(mockLookupFactory.close()).andReturn(true).anyTimes();
    EasyMock.expect(mockLookupFactory.get()).andReturn(mockLookup).anyTimes();
    EasyMock.replay(lookupReferencesManager, mockLookupFactory);
    mapper.setInjectableValues(new InjectableValues.Std().addValue(
        LookupReferencesManager.class,
        lookupReferencesManager
    ));
    lookupExtractor = new LookupDelegator(lookupReferencesManager, name);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(lookupReferencesManager);
    EasyMock.reset(lookupReferencesManager, mockLookup);
  }

  @Test
  public void testApply()
  {
    EasyMock.expect(mockLookup.apply("testKey")).andReturn("value").times(1);
    EasyMock.replay(mockLookup);
    Assert.assertEquals("value", lookupExtractor.apply("testKey"));
  }

  @Test
  public void testUnapply()
  {
    EasyMock.expect(mockLookup.unapply("value")).andReturn(Arrays.asList("key2","key")).times(1);
    EasyMock.replay(mockLookup);
    Assert.assertEquals(Arrays.asList("key2", "key"), lookupExtractor.unapply("value"));
  }

  @Test
  public void testGetCacheKey()
  {
    EasyMock.expect(mockLookup.getCacheKey()).andReturn(new byte[0]).times(1);
    EasyMock.replay(mockLookup);
    Assert.assertArrayEquals(new byte[0], lookupExtractor.getCacheKey());
  }

  @Test
  public void testApplyAll()
  {
    EasyMock.expect(mockLookup.applyAll(EasyMock.anyObject(Iterable.class))).andReturn(Collections.EMPTY_MAP).times(1);
    EasyMock.replay(mockLookup);
    Assert.assertEquals(Collections.EMPTY_MAP, lookupExtractor.applyAll(Arrays.asList("key")));
  }

  @Test
  public void testUnapplyAll()
  {
    EasyMock.expect(mockLookup.unapplyAll(EasyMock.anyObject(Iterable.class))).andReturn(Collections.EMPTY_MAP).times(1);
    EasyMock.replay(mockLookup);
    Assert.assertEquals(Collections.EMPTY_MAP, lookupExtractor.unapplyAll(Arrays.asList("key")));
  }
}
