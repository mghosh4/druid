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

package io.druid.segment;

import com.metamx.emitter.EmittingLogger;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReferenceCountingSegment implements Segment
{
  private static final EmittingLogger log = new EmittingLogger(ReferenceCountingSegment.class);

  private final Segment baseSegment;

  private final Object lock = new Object();

  private volatile int numReferences = 0;
  private volatile boolean isClosed = false;
  
  private volatile int NumPerSecReferences = 0;
  private volatile long lastUpdatedTime = System.currentTimeMillis();

  public ReferenceCountingSegment(Segment baseSegment)
  {
    this.baseSegment = baseSegment;
  }

  public Segment getBaseSegment()
  {
    synchronized (lock) {
      if (isClosed) {
        return null;
      }
      log.info("Get base segment, %s", baseSegment.getIdentifier());
      return baseSegment;
    }
  }

  public int getNumReferences()
  {
	log.info("number of references %d", numReferences);
    return numReferences;
  }
  
  public int getNumPerSecReferences(){
	  long currentTime = System.currentTimeMillis();
	  if(currentTime-lastUpdatedTime >1000){
		  NumPerSecReferences = numReferences;
		  lastUpdatedTime = currentTime;
		  return NumPerSecReferences;
	  }
	  return NumPerSecReferences;
  }

  public boolean isClosed()
  {
    return isClosed;
  }

  @Override
  public String getIdentifier()
  {
    synchronized (lock) {
      if (isClosed) {
        return null;
      }
      log.info("get Identifier", baseSegment.getIdentifier());
      return baseSegment.getIdentifier();
    }
  }

  @Override
  public Interval getDataInterval()
  {
    synchronized (lock) {
      if (isClosed) {
        return null;
      }
      log.info("get data interval", baseSegment.getDataInterval().toString());
      return baseSegment.getDataInterval();
    }
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    synchronized (lock) {
      if (isClosed) {
        return null;
      }

      return baseSegment.asQueryableIndex();
    }
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    synchronized (lock) {
      if (isClosed) {
        return null;
      }

      return baseSegment.asStorageAdapter();
    }
  }

  @Override
  public void close() throws IOException
  {
    synchronized (lock) {
      if (isClosed) {
        log.info("Failed to close, %s is closed already", baseSegment.getIdentifier());
        return;
      }

      if (numReferences > 0) {
        log.info("%d references to %s still exist. Decrementing.", numReferences, baseSegment.getIdentifier());

        decrement();
      } else {
        log.info("Closing %s", baseSegment.getIdentifier());
        innerClose();
      }
    }
  }

  public Closeable increment()
  {
    synchronized (lock) {
      if (isClosed) {
        return null;
      }

      numReferences++;
      log.info("baseSegment %s",baseSegment.getIdentifier());
      log.info("increase number of references to %d", numReferences);
      final AtomicBoolean decrementOnce = new AtomicBoolean(false);
      return new Closeable()
      {
        @Override
        public void close() throws IOException
        {
          if (decrementOnce.compareAndSet(false, true)) {
            decrement();
          }
        }
      };
    }
  }

  private void decrement()
  {
    synchronized (lock) {
      if (isClosed) {
        return;
      }
      log.info("decrement count for segment %s",getIdentifier());
      if (--numReferences < 0) {
        try {
          log.info("segment %s reference count less than 0, close it",getIdentifier());
          innerClose();
        }
        catch (Exception e) {
          log.error("Unable to close queryable index %s", getIdentifier());
        }
      }
    }
  }

  private void innerClose() throws IOException
  {
    synchronized (lock) {
      log.info("Closing %s, numReferences: %d", baseSegment.getIdentifier(), numReferences);

      isClosed = true;
      baseSegment.close();
    }
  }
}
