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

package io.druid.query.dimension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.LookupExtractor;
import io.druid.query.extraction.MapLookupExtractor;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;

@RunWith(JUnitParamsRunner.class)
public class LookupDimensionSpecTest
{
  private static final LookupExtractor lookupMap = new MapLookupExtractor(
      ImmutableMap.of("key", "value", "key2", "value2"), true);

  private DimensionSpec lookupDimSpec;

  @Before
  public void setUp()
  {
    lookupDimSpec = new LookupDimensionSpec("dimName", "outputName", lookupMap, false, null);
  }

  @Parameters
  @Test
  public void testSerDesr(DimensionSpec lookupDimSpec) throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    String serLookup = mapper.writeValueAsString(lookupDimSpec);
    Assert.assertEquals(lookupDimSpec, mapper.reader(DimensionSpec.class).readValue(serLookup));
  }

  private Object[] parametersForTestSerDesr()
  {
    return new Object[]{
        new LookupDimensionSpec("dimName", "outputName", lookupMap, true, null),
        new LookupDimensionSpec("dimName", "outputName", lookupMap, false, "Missing_value"),
        new LookupDimensionSpec("dimName", "outputName", lookupMap, false, null)
    };
  }

  @Test(expected = Exception.class)
  public void testExceptionWhenRetainMissingAndReplaceMissing()
  {
    new LookupDimensionSpec("dimName", "outputName", lookupMap, true, "replace");
  }

  @Test
  public void testGetDimension()
  {
    Assert.assertEquals("dimName", lookupDimSpec.getDimension());
  }

  @Test
  public void testGetOutputName()
  {
    Assert.assertEquals("outputName", lookupDimSpec.getOutputName());
  }

  @Test
  @Parameters
  public void testApply(DimensionSpec dimensionSpec, String expectedString)
  {
    Assert.assertEquals(expectedString, dimensionSpec.getExtractionFn().apply("not there"));
    Assert.assertEquals("value", dimensionSpec.getExtractionFn().apply("key"));
    Assert.assertEquals("value2", dimensionSpec.getExtractionFn().apply("key2"));
  }
  public Object[] parametersForTestApply(){
    return new Object[]{
        new Object[]{new LookupDimensionSpec("dimName", "outputName", lookupMap, true, null), "not there"},
        new Object[]{new LookupDimensionSpec("dimName", "outputName", lookupMap, false, "Missing_value"), "Missing_value"},
        new Object[]{new LookupDimensionSpec("dimName", "outputName", lookupMap, false, null), null}
    };
  }

  @Test
  public void testGetExtractionFnWithReplaceMissing()
  {
    lookupDimSpec = new LookupDimensionSpec("dimName", "outputName", lookupMap, false, "Missing_value");
    Assert.assertEquals("Missing_value", lookupDimSpec.getExtractionFn().apply("not there"));
    Assert.assertEquals("value", lookupDimSpec.getExtractionFn().apply("key"));
  }

  @Test
  public void testGetExtractionFnWithRetainMissing()
  {
    lookupDimSpec = new LookupDimensionSpec("dimName", "outputName", lookupMap, true, null);
    Assert.assertEquals("not there", lookupDimSpec.getExtractionFn().apply("not there"));
    Assert.assertEquals("value", lookupDimSpec.getExtractionFn().apply("key"));
  }

  @Test
  @Parameters
  public void testGetCacheKey(DimensionSpec dimensionSpec, boolean expectedResult)
  {
    Assert.assertEquals(expectedResult, Arrays.equals(lookupDimSpec.getCacheKey(), dimensionSpec.getCacheKey()));
  }

  private Object[] parametersForTestGetCacheKey()
  {
    return new Object[]{
        new Object[]{new LookupDimensionSpec("dimName", "outputName", lookupMap, true, null), false},
        new Object[]{new LookupDimensionSpec("dimName", "outputName", lookupMap, false, "Missing_value"), false},
        new Object[]{new LookupDimensionSpec("dimName", "outputName2", lookupMap, false, null), false},
        new Object[]{new LookupDimensionSpec("dimName2", "outputName2", lookupMap, false, null), false},
        new Object[]{new LookupDimensionSpec("dimName", "outputName", lookupMap, false, null), true}
    };
  }

  @Test
  public void testPreservesOrdering()
  {
    Assert.assertFalse(lookupDimSpec.preservesOrdering());
  }

  @Test
  public void testIsOneToOne()
  {
    Assert.assertEquals(lookupDimSpec.getExtractionFn().getExtractionType(), ExtractionFn.ExtractionType.ONE_TO_ONE);
  }
}
