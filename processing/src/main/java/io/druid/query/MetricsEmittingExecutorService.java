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

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.metamx.common.Pair;
import org.apache.commons.lang3.tuple.Triple;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.joda.time.DateTime;

import java.util.List;
import java.util.concurrent.Callable;

public class MetricsEmittingExecutorService extends ForwardingListeningExecutorService
{
  private final ListeningExecutorService delegate;
  private final ServiceEmitter emitter;
  private final ServiceMetricEvent.Builder metricBuilder;

  public MetricsEmittingExecutorService(
      ListeningExecutorService delegate,
      ServiceEmitter emitter,
      ServiceMetricEvent.Builder metricBuilder
  )
  {
    super();

    this.delegate = delegate;
    this.emitter = emitter;
    this.metricBuilder = metricBuilder;
  }

  @Override
  protected ListeningExecutorService delegate()
  {
    return delegate;
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> tCallable)
  {
    emitMetrics();
    return delegate.submit(tCallable);
  }

  @Override
  public void execute(Runnable runnable)
  {
    emitMetrics();
    delegate.execute(runnable);
  }

  public List<Pair<String, Long>> getQueuedTasks()
  {
    if (delegate instanceof PrioritizedExecutorService)
      return ((PrioritizedExecutorService) delegate).getQueuedTasks();

    return null;
  }

  public int getQueueSize()
  {
    if (delegate instanceof PrioritizedExecutorService) 
      return ((PrioritizedExecutorService) delegate).getQueueSize();
    
    return 0;
  }

  public int getActiveTaskCount()
  {
    if (delegate instanceof PrioritizedExecutorService) 
      return ((PrioritizedExecutorService) delegate).getActiveTaskCount();
    
    return 0;
  }

  public int getCorePoolSize()
  {
    if (delegate instanceof PrioritizedExecutorService)
      return ((PrioritizedExecutorService) delegate).getCorePoolSize();

    return 0;
  }

  public Iterable<Triple<DateTime, String, Long>> getActiveRunList()
  {
    if (delegate instanceof PrioritizedExecutorService)
      return ((PrioritizedExecutorService) delegate).getActiveRunList();

    return null;
  }

  public boolean isNewTaskAdded()
  {
    if (delegate instanceof PrioritizedExecutorService)
      return ((PrioritizedExecutorService) delegate).isNewTaskAdded();

    return false;
  }

  private void emitMetrics()
  {
    if (delegate instanceof PrioritizedExecutorService) {
      emitter.emit(metricBuilder.build("segment/scan/pending", (((PrioritizedExecutorService) delegate).getQueueSize()) + ((PrioritizedExecutorService) delegate).getActiveTaskCount()));
    }
  }
}
