package io.druid.server.coordinator.helper;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.common.logger.Logger;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadPeonCallback;
import io.druid.server.coordinator.ReplicationThrottler;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 */
public class DruidCoordinatorSegmentGarbageCollector implements DruidCoordinatorHelper
{
  private final double gcThreshold;
  private final boolean isGCAggressive;
  private final DruidCoordinator coordinator;

  private static final String droppedCount = "droppedCount";
  private static final Logger log = new Logger(DruidCoordinatorSegmentGarbageCollector.class);

  public DruidCoordinatorSegmentGarbageCollector(
      DruidCoordinator coordinator,
      double gcThreshold,
      boolean isGCAggressive
  )
  {
    this.coordinator = coordinator;
    this.gcThreshold = gcThreshold;
    this.isGCAggressive = isGCAggressive;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()) {
      for (ServerHolder holder : serverQueue) {
        long currSize = holder.getCurrServerSize();
        double maxSize = holder.getMaxSize().doubleValue();
        int iter = 0;
        while (currSize / maxSize > gcThreshold) {
          log.info("[GETAFIX GC] Found server %s(%s) with size %s/%s(%s), which is more than gcThreshold. Dropping iter %s...",
                  holder.getServer().getName(), holder.getServer().getHost(), currSize, holder.getMaxSize(), currSize/maxSize, iter);
          // find the segment that is owned by this server holder, and has the lowest weighted access count
          DataSegment segmentToDrop = null;
          Long lowestWeightedAccessCount = Long.MAX_VALUE;
          for (Map.Entry<DataSegment, Long> entry : coordinator.getWeightedAccessCounts().entrySet()) {
            if (entry.getValue() < lowestWeightedAccessCount) {
              HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable = coordinator.getRoutingTable();
              if (!routingTable.containsKey(entry.getKey())) {
                log.error("[GETAFIX GC] Segment not found in routing table");
                continue;
              }

              if (!routingTable.get(entry.getKey()).containsKey(holder.getServer().getMetadata())) {
                log.info("[GETAFIX GC] Server doesn't have this segment");
                continue;
              }

              segmentToDrop = entry.getKey();
              lowestWeightedAccessCount = entry.getValue();
            }
          }

          if (segmentToDrop == null) {
            log.error("[GETAFIX GC] Cannot find a segment to drop");
            return params;
          }

          // other params
          final List<String> tierNameList = Lists.newArrayList(params.getDruidCluster().getTierNames());
          if (tierNameList.size() == 0) {
            log.error("Cluster has multiple tiers! Check your cluster configuration!");
            return params;
          }
          final String tier = tierNameList.get(0);

          log.info("[GETAFIX GC] Dropping " + segmentToDrop.getIdentifier() + " from " + holder.getServer().getHost());
          drop(
                  params.getReplicationManager(),
                  tier,
                  holder,
                  segmentToDrop
          );
          currSize -= segmentToDrop.getSize();
          if (!this.isGCAggressive) {
            log.info("[GETAFIX GC] Not Aggressive");
            break;
          }
          iter++;
        }
      }
    }


    return params;
  }

  private CoordinatorStats drop(
          final ReplicationThrottler replicationManager,
          final String tier,
          final List<ServerHolder> serverHolderList,
          final DataSegment segment,
          final long numReplicantsToRemove
  )
  {
    log.info("Remove Segment [%s] [%d]", segment.getIdentifier(), numReplicantsToRemove);
    CoordinatorStats stats = new CoordinatorStats();

    // Pick the server which has the maximum number of segments for load balance
    Map<Number, ServerHolder> segmentCountMap = new TreeMap<Number, ServerHolder>(Collections.reverseOrder());
    for (ServerHolder serverHolder : serverHolderList)
      if (serverHolder.getServer().getSegment(segment.getIdentifier()) != null)
        segmentCountMap.put(serverHolder.getServer().getSegments().size(), serverHolder);

    long numReplicants = numReplicantsToRemove;
    for (Map.Entry<Number, ServerHolder> entry : segmentCountMap.entrySet())
    {
      //log.info("Removing Segment from [%s] because it has [%d] segments", entry.getValue().getServer().getMetadata().toString(), entry.getKey().intValue());
      final ServerHolder holder = entry.getValue();
      stats.addToTieredStat(droppedCount, tier, 0);

      if (holder.isServingSegment(segment)) {
        replicationManager.registerReplicantTermination(
                tier,
                segment.getIdentifier(),
                holder.getServer().getHost()
        );
      }

      holder.getPeon().dropSegment(
              segment,
              new LoadPeonCallback()
              {
                @Override
                public void execute()
                {
                  replicationManager.unregisterReplicantTermination(
                          tier,
                          segment.getIdentifier(),
                          holder.getServer().getHost()
                  );
                }
              }
      );

      --numReplicants;
      stats.addToTieredStat(droppedCount, tier, 1);
      log.info("Removed Segment [%s]", segment.getIdentifier());

      if (numReplicants == 0)
        break;
    }
    return stats;
  }

  private CoordinatorStats drop(
          final ReplicationThrottler replicationManager,
          final String tier,
          final ServerHolder holder,
          final DataSegment segment
  )
  {
    log.info("Remove Segment [%s] from [%s]", segment.getIdentifier(), holder.getServer().getHost());
    CoordinatorStats stats = new CoordinatorStats();

    //log.info("Removing Segment from [%s] because it has [%d] segments", entry.getValue().getServer().getMetadata().toString(), entry.getKey().intValue());
    stats.addToTieredStat(droppedCount, tier, 0);

    if (holder.isServingSegment(segment)) {
      replicationManager.registerReplicantTermination(
              tier,
              segment.getIdentifier(),
              holder.getServer().getHost()
      );
    }

    holder.getPeon().dropSegment(
            segment,
            new LoadPeonCallback()
            {
              @Override
              public void execute()
              {
                replicationManager.unregisterReplicantTermination(
                        tier,
                        segment.getIdentifier(),
                        holder.getServer().getHost()
                );
              }
            }
    );

    stats.addToTieredStat(droppedCount, tier, 1);
    log.info("Removed Segment [%s]", segment.getIdentifier());

    return stats;
  }
}
