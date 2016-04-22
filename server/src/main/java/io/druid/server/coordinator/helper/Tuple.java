package io.druid.server.coordinator.helper;

import io.druid.timeline.DataSegment;

public class Tuple{
  public final DataSegment segment;
  public final double weight;

  public Tuple(DataSegment segment, double weight)
  {
    this.segment = segment;
    this.weight = weight;
  }
}