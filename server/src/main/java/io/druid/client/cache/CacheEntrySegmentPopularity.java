package io.druid.client.cache;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Created by xiaoyaoqian on 9/23/16.
 */
public class CacheEntrySegmentPopularity
{
  private final ByteBuffer key;
  private final byte[] value;
  private final String segmentIdentifier;
  private final Float popularity;

  public CacheEntrySegmentPopularity(String segmentIdentifier, ByteBuffer key, byte[] value, Float popularity)
  {
    this.segmentIdentifier = segmentIdentifier;
    this.key = key;
    this.value = value;
    this.popularity = popularity;
  }

  public String getSegmentIdentifier()
  {
    return segmentIdentifier;
  }

  public Float getPopularity()
  {
    return popularity;
  }

  public ByteBuffer getKey()
  {
    return key;
  }

  public byte[] getValue()
  {
    return value;
  }

  public int getEntrySize()
  {
    return key.remaining() + value.length;
  }

  public static class SegmentPopularityComparator implements Comparator<CacheEntrySegmentPopularity> {
    private static final float TOLERANCE = 0.0001f;
    @Override
    public int compare(CacheEntrySegmentPopularity o1, CacheEntrySegmentPopularity o2)
    {
      if (o1.getPopularity() < o2.getPopularity()) {
        return -1;
      } else if (o1.getPopularity() > o2.getPopularity()) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
