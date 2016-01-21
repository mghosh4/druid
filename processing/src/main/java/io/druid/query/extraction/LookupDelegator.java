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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LookupDelegator extends LookupExtractor
{
  private final LookupExtractor delegate;
  private final LookupReferencesManager lookupReferencesManager;
  @JsonProperty
  private final String lookupName;

  @JsonCreator
  public LookupDelegator(
      @JacksonInject LookupReferencesManager lookupReferencesManager,
      @JsonProperty("lookupName") String lookupName
  )
  {
    this.lookupName = Preconditions.checkNotNull(lookupName, "lookupName can not be null");
    this.lookupReferencesManager = Preconditions.checkNotNull(
        lookupReferencesManager,
        "lookup reference manager can not be found"
    );
    LookupExtractorFactory lookupExtractorFactory = Preconditions.checkNotNull(
        this.lookupReferencesManager.get(this.lookupName),
        "can not find lookup registered under [%s]",
        lookupName
    );
    this.delegate = Preconditions.checkNotNull(lookupExtractorFactory.get(), "Factory returned null");
  }

  @Override
  public String apply(String key)
  {
    return delegate.apply(key);
  }

  @Override
  public List<String> unapply(String value)
  {
    return delegate.unapply(value);
  }

  @Override
  public byte[] getCacheKey()
  {
    return delegate.getCacheKey();
  }

  @Override
  public Map<String, String> applyAll(Iterable<String> keys)
  {
    return delegate.applyAll(keys);
  }

  @Override
  public Map<String, List<String>> unapplyAll(Iterable<String> values)
  {
    return delegate.unapplyAll(values);
  }

  @Override
  public boolean isOneToOne()
  {
    return delegate.isOneToOne();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LookupDelegator)) {
      return false;
    }

    LookupDelegator that = (LookupDelegator) o;

    return lookupName.equals(that.lookupName);

  }

  @Override
  public int hashCode()
  {
    return lookupName.hashCode();
  }

}
