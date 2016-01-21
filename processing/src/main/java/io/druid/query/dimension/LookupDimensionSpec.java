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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.metamx.common.StringUtils;
import io.druid.query.extraction.DimExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.LookupExtractor;
import io.druid.query.filter.DimFilterCacheHelper;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;

public class LookupDimensionSpec implements DimensionSpec
{
  private static final byte CACHE_TYPE_ID = 0x4;

  @JsonProperty
  private final String dimension;

  @JsonProperty
  private final String outputName;

  @JsonProperty
  private final LookupExtractor lookup;

  @JsonProperty
  private final boolean retainMissingValues;

  @JsonProperty
  private final String replaceMissingWith;

  private final ExtractionFn extractionFn;

  @JsonCreator
  public LookupDimensionSpec(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("outputName") String outputName,
      @JsonProperty("lookup") LookupExtractor lookupInput,
      @JsonProperty("retainMissingValues") final boolean retainMissingValues,
      @JsonProperty("replaceMissingWith") final String replaceMissingWith
  )
  {
    this.retainMissingValues = retainMissingValues;
    this.replaceMissingWith = Strings.emptyToNull(replaceMissingWith);
    Preconditions.checkArgument(!(retainMissingValues && !Strings.isNullOrEmpty(replaceMissingWith)), "Cannot specify a [replaceMissingValue] and set [retainMissingValue] to true");
    this.dimension = Preconditions.checkNotNull(dimension, "dimension can not be Null");
    this.outputName = Preconditions.checkNotNull(outputName, "outputName can not be Null");
    this.lookup = Preconditions.checkNotNull(lookupInput, "lookup can not be Null");
    this.extractionFn = new DimExtractionFn()
    {
      @Override
      public byte[] getCacheKey()
      {
        return lookup.getCacheKey();
      }

      @Override
      public String apply(String value)
      {
        final String retVal = Strings.emptyToNull(lookup.apply(value));
        if (retainMissingValues)
        {
          return retVal == null ? Strings.emptyToNull(value) : retVal;
        } else {
        return retVal == null ? replaceMissingWith : retVal;
        }
      }

      @Override
      public boolean preservesOrdering()
      {
        return false;
      }

      @Override
      public ExtractionType getExtractionType()
      {
        return lookup.isOneToOne() ? ExtractionType.ONE_TO_ONE : ExtractionType.MANY_TO_ONE;
      }
    };
  }

  @Override
  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @Override
  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @JsonProperty
  public LookupExtractor getLookup()
  {
    return lookup;
  }

  @Override
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public DimensionSelector decorate(DimensionSelector selector)
  {
    return selector;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    byte[] dimExtractionFnBytes = extractionFn.getCacheKey();
    byte[] outputNameBytes = StringUtils.toUtf8(outputName);
    byte[] replaceWithBytes = StringUtils.toUtf8(Strings.nullToEmpty(replaceMissingWith));


    return ByteBuffer.allocate(6 + dimensionBytes.length + outputNameBytes.length + dimExtractionFnBytes.length + replaceWithBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(dimensionBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(outputNameBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(dimExtractionFnBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(replaceWithBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(retainMissingValues == true ? (byte) 1 : (byte) 0)
                     .array();
  }

  @Override
  public boolean preservesOrdering()
  {
    return extractionFn.preservesOrdering();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LookupDimensionSpec)) {
      return false;
    }

    LookupDimensionSpec that = (LookupDimensionSpec) o;

    if (retainMissingValues != that.retainMissingValues) {
      return false;
    }
    if (!getDimension().equals(that.getDimension())) {
      return false;
    }
    if (!getOutputName().equals(that.getOutputName())) {
      return false;
    }
    if (!getLookup().equals(that.getLookup())) {
      return false;
    }
    return replaceMissingWith != null
           ? replaceMissingWith.equals(that.replaceMissingWith)
           : that.replaceMissingWith == null;

  }

  @Override
  public int hashCode()
  {
    int result = getDimension().hashCode();
    result = 31 * result + getOutputName().hashCode();
    result = 31 * result + getLookup().hashCode();
    result = 31 * result + (retainMissingValues ? 1 : 0);
    result = 31 * result + (replaceMissingWith != null ? replaceMissingWith.hashCode() : 0);
    return result;
  }
}
