package io.druid.server.coordinator.helper;

import io.druid.timeline.DataSegment;

public class Tuple{
  public final DataSegment segment;
  public final long weight;

  public Tuple(DataSegment segment, long weight)
  {
    this.segment = segment;
    this.weight = weight;
  }
}