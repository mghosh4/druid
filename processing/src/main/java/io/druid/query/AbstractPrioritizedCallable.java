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

package io.druid.query;

public abstract class AbstractPrioritizedCallable<V> implements PrioritizedCallable<V>
{
  private final int priority;
  private final String queryType;
  private final long duration;

  public AbstractPrioritizedCallable(int priority, String queryType, long duration)
  {
    this.priority = priority;
    this.queryType = queryType;
    this.duration = duration;
  }

  @Override
  public int getPriority()
  {
    return priority;
  }

  @Override
  public String getQueryType() { return  queryType; }

  @Override
  public long getDuration() { return duration; }
}
