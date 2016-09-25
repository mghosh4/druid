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

import com.metamx.common.logger.Logger;

import java.nio.ByteBuffer;
import java.util.Map;

/**
*/
class ByteCountingLRUMap extends ByteCountingMap
{
  private static final Logger log = new Logger(ByteCountingLRUMap.class);

  public ByteCountingLRUMap(long sizeInBytes)
  {
    super(sizeInBytes);
  }

  public ByteCountingLRUMap(int initialSize, int logEvictionCount, long sizeInBytes)
  {
    super(initialSize, logEvictionCount, sizeInBytes);
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<ByteBuffer, byte[]> eldest)
  {
    if (numBytes > sizeInBytes) {
      ++evictionCount;
      if (logEvictions && evictionCount % logEvictionCount == 0) {
        log.info(
            "Evicting %,dth element.  Size[%,d], numBytes[%,d], averageSize[%,d]",
            evictionCount,
            size(),
            numBytes,
            numBytes / size()
        );
      }

      numBytes -= eldest.getKey().remaining() + eldest.getValue().length;
      return true;
    }
    return false;
  }
}
