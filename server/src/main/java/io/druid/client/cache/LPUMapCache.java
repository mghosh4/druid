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
package io.druid.client.cache;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import org.apache.commons.codec.binary.Hex;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Least Popularly Used(LPU) Map Cache
 */
public class LPUMapCache implements Cache
{
  private static final Logger log = new Logger(LPUMapCache.class);
  public static Cache create(long sizeInBytes)
  {
    return new LPUMapCache(new ByteCountingMap(sizeInBytes));
  }

  private final Map<ByteBuffer, byte[]> baseMap;
  private final ByteCountingMap byteCountingMap;

  private final Map<String, byte[]> namespaceId;
  private final ConcurrentMap<String, String> reverseNamespaceId;
  private final AtomicInteger ids;

  private final Object clearLock = new Object();

  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong missCount = new AtomicLong(0);
  private final AtomicLong evictionCount = new AtomicLong(0);

  private volatile Map<String, Double> segmentPopularitySnapshot;

  LPUMapCache(
      ByteCountingMap byteCountingMap
  )
  {
    this.byteCountingMap = byteCountingMap;
    this.baseMap = Collections.synchronizedMap(byteCountingMap);

    this.segmentPopularitySnapshot = Maps.newHashMap();

    namespaceId = Maps.newHashMap();
    reverseNamespaceId = Maps.newConcurrentMap();
    ids = new AtomicInteger();
  }

  @Override
  public CacheStats getStats()
  {
    return new CacheStats(
        hitCount.get(),
        missCount.get(),
        byteCountingMap.size(),
        byteCountingMap.getNumBytes(),
        evictionCount.get(),
        0,
        0
    );
  }

  @Override
  public byte[] get(NamedKey key)
  {
    final byte[] retVal;
    synchronized (clearLock) {
      retVal = baseMap.get(computeKey(getNamespaceId(key.namespace), key.key));
    }
    if (retVal == null) {
      missCount.incrementAndGet();
    } else {
      hitCount.incrementAndGet();
    }
    return retVal;
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    synchronized (clearLock) {
      ByteBuffer newEntryKey = computeKey(getNamespaceId(key.namespace), key.key);
      int newEntrySize = newEntryKey.remaining() + value.length;

      // If adding the new entry could cause cache size to exceed its limit, we should decide to evict least popular entries
      if (byteCountingMap.getNumBytes() + newEntrySize > byteCountingMap.getSizeInBytes()) {
        PriorityQueue<CacheEntrySegmentPopularity> pq = new PriorityQueue<>(baseMap.size(), new CacheEntrySegmentPopularity.SegmentPopularityComparator());
        for (Entry<ByteBuffer, byte[]> cacheEntry : baseMap.entrySet()) {
          // Retrieve the namespaceId byte array
          byte[] namespaceIdBytes = new byte[4];
          cacheEntry.getKey().get(namespaceIdBytes);
          cacheEntry.getKey().rewind();

          // Lookup the namespaceId byte array to get the segmentIdentifier string
          String segmentIdentifier = reverseNamespaceId.get(Hex.encodeHexString(namespaceIdBytes));
          if (segmentIdentifier == null) {
            throw new IllegalStateException("SegmentIdentifier not found in reverse namespaceId lookup, but exist in cache");
          }

          // Lookup segment popularity, populate priority queue
          Double popularity = segmentPopularitySnapshot.get(segmentIdentifier);
          if (popularity == null) {
            throw new IllegalStateException("SegmentIdentifier not found in popularity snapshot, but exist in cache");
          }
          pq.add(new CacheEntrySegmentPopularity(segmentIdentifier, cacheEntry.getKey(), cacheEntry.getValue(), popularity));
        }

        int totalEvictionSize = 0;
        List<CacheEntrySegmentPopularity> toEvict = new ArrayList<>();
        while (byteCountingMap.getNumBytes() + newEntrySize - totalEvictionSize > byteCountingMap.getSizeInBytes()) {
          CacheEntrySegmentPopularity leastPopularSegment = pq.poll();
          toEvict.add(leastPopularSegment);
          totalEvictionSize += leastPopularSegment.getEntrySize();
        }

        // Evict
        for (CacheEntrySegmentPopularity entryToEvict : toEvict) {
          evictionCount.incrementAndGet();
          log.info(
              "Evicting segment %s with popularity=%s",
              entryToEvict.getSegmentIdentifier(),
              entryToEvict.getPopularity()
          );
          baseMap.remove(entryToEvict.getKey());
        }
      }

      baseMap.put(newEntryKey, value);
    }
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    Map<NamedKey, byte[]> retVal = Maps.newHashMap();
    for (NamedKey key : keys) {
      final byte[] value = get(key);
      if (value != null) {
        retVal.put(key, value);
      }
    }
    return retVal;
  }

  @Override
  public void close(String namespace)
  {
    byte[] idBytes;
    synchronized (namespaceId) {
      idBytes = getNamespaceId(namespace);
      if (idBytes == null) {
        return;
      }

      namespaceId.remove(namespace);
      reverseNamespaceId.remove(Hex.encodeHexString(idBytes));
    }
    synchronized (clearLock) {
      Iterator<ByteBuffer> iter = baseMap.keySet().iterator();
      List<ByteBuffer> toRemove = Lists.newLinkedList();
      while (iter.hasNext()) {
        ByteBuffer next = iter.next();

        if (next.get(0) == idBytes[0]
            && next.get(1) == idBytes[1]
            && next.get(2) == idBytes[2]
            && next.get(3) == idBytes[3]) {
          toRemove.add(next);
        }
      }
      for (ByteBuffer key : toRemove) {
        baseMap.remove(key);
      }
    }
  }

  private byte[] getNamespaceId(final String identifier)
  {
    synchronized (namespaceId) {
      byte[] idBytes = namespaceId.get(identifier);
      if (idBytes != null) {
        return idBytes;
      }

      idBytes = Ints.toByteArray(ids.getAndIncrement());
      namespaceId.put(identifier, idBytes);
      reverseNamespaceId.put(Hex.encodeHexString(idBytes), identifier);
      return idBytes;
    }
  }

  private ByteBuffer computeKey(byte[] idBytes, byte[] key)
  {
    final ByteBuffer retVal = ByteBuffer.allocate(key.length + 4).put(idBytes).put(key);
    retVal.rewind();
    return retVal;
  }

  public boolean isLocal()
  {
    return true;
  }

  @Override
  public void doMonitor(ServiceEmitter emitter)
  {
    // No special monitoring
  }

  @Override
  public void setSegmentsPopularitiesSnapshot(Map<String, Double> segmentsPopularitiesSnapshot)
  {
    this.segmentPopularitySnapshot = segmentsPopularitiesSnapshot;
  }
}
