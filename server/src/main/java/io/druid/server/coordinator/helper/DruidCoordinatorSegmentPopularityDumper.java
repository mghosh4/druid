package io.druid.server.coordinator.helper;

import io.druid.metadata.MetadataSegmentManager;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;

/**
 * Dump the segment popularity to metadata storage
 */
public class DruidCoordinatorSegmentPopularityDumper implements DruidCoordinatorHelper
{
  private final DruidCoordinator coordinator;
  private final MetadataSegmentManager metadataSegmentManager;

  public DruidCoordinatorSegmentPopularityDumper(
      DruidCoordinator coordinator,
      MetadataSegmentManager metadataSegmentManager
  )
  {
    this.coordinator = coordinator;
    this.metadataSegmentManager = metadataSegmentManager;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    metadataSegmentManager.updateSegmentsPopularities(coordinator.getWeightedAccessCounts());
    return params;
  }
}
