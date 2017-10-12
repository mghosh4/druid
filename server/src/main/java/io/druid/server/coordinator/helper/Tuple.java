package io.druid.server.coordinator.helper;

import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;

public class Tuple{
  public final DataSegment segment;
  public final long weight;
  public DruidServerMetadata metadata;

  public Tuple(DataSegment segment, long weight)
  {
    this.segment = segment;
    this.weight = weight;
  }

  public Tuple(DataSegment segment, long weight, DruidServerMetadata metadata)
  {
    this.segment = segment;
    this.weight = weight;
    this.metadata = metadata;
  }
}
