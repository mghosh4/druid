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

package io.druid.server.coordinator.helper;

import com.metamx.emitter.EmittingLogger;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;

import io.druid.server.coordinator.BalancerStrategy;
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
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.joda.time.DateTime;

public class DruidCoordinatorSegmentReplicator implements DruidCoordinatorHelper
{
  private final DruidCoordinator coordinator;

  private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorSegmentReplicator.class);
  private static final String assignedCount = "assignedCount";
  private static final String droppedCount = "droppedCount";

  public DruidCoordinatorSegmentReplicator(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    log.info("Starting replication. Getting Segment Popularity");
    final CoordinatorStats stats = new CoordinatorStats();
    HashMap<DataSegment,Number> insertList = new HashMap<DataSegment,Number>();
    HashMap<DataSegment,Number> removeList = new HashMap<DataSegment,Number>();

    // Acquire Query Workload in the last window
    Multiset<DataSegment> segments = HashMultiset.create();
    calculateSegmentCounts(segments);

    // Calculate the popularity map
    //TODO: Implement getSegmentPopularityMap
    HashMap<DataSegment, Number> weightedAccessCounts = coordinator.getSegmentPopularityMap();
    calculateWeightedAccessCounts(params, segments, weightedAccessCounts, removeList);
    coordinator.setSegmentPopularityMap(weightedAccessCounts);

    // Calculate replication based on popularity
    calculateReplication(params, weightedAccessCounts, insertList, removeList);
    
    // Manage replicas
    manageReplicas(params, insertList, removeList, stats);

    return params.buildFromExisting() 
	    .withCoordinatorStats(stats) 
	    .build();
  }

  private void calculateSegmentCounts(Multiset<DataSegment> segments)
  {
    log.info("Starting replication. Getting Segment Popularity");
  }

  private void calculateWeightedAccessCounts(DruidCoordinatorRuntimeParams params, Multiset<DataSegment> segments, HashMap<DataSegment, Number> weightedAccessCounts, HashMap<DataSegment, Number> removeList)
  {
    log.info("Calculating Weighted Access Counts for Segments");

    // Handle those segments which are in Coordinator's map but not in segments collected from query
    for (Map.Entry<DataSegment,Number> entry : weightedAccessCounts.entrySet())
    	if (segments.contains(entry.getKey()) == false)
		weightedAccessCounts.put(entry.getKey(), 0.5 * entry.getValue().doubleValue())

    for (Entry<DataSegment> segment : segments.entrySet())
    {
	DataSegment dataSegment = segment.getElement();
    	int segmentCount = segment.getCount();

	if (weightedAccessCounts.containsKey(dataSegment) == false)
		weightedAccessCounts.put(dataSegment, (double)segmentCount);
	else
	{
		double popularity = weightedAccessCounts.get(dataSegment);
		weightedAccessCounts.put(dataSegment, segmentCount + 0.5 * popularity);
	}
    }

    // Remove segments with counts less than a threshold from weightedAccessCounts. Also add it to removeList
    for (Map.Entry<DataSegment,Number> entry : weightedAccessCounts.entrySet())
    {
	if (entry.getValue().doubleValue() < MIN_THRESHOLD)
	{
		DataSegment segment = entry.getKey();
		removelist.push(segment, params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier()));
		weightedAccessCount.remove(segment);
	}
    }
  }

  // Implemented Adaptive Strategy
  // TODO: Best Fit Strategy
  private void calculateReplication(DruidCoordinatorRuntimeParams params, HashMap<DataSegment, Number> weightedAccessCounts, HashMap<DataSegment,Number> insertList, HashMap<DataSegment,Number> removeList)
  {
    log.info("Calculating Replication for Segments");
    int historicalNodeCount = 0;
    for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier())
    	historicalNodeCount += serverQueue.size(); 
    
    int totalWeightCount = 0;
    for (Number number : weightedAccessCounts.values())
    	totalWeightCount += number.intValue();
    
    for (Map.Entry<DataSegment, Number> entry : weightedAccessCounts.entrySet())
    {
    	DataSegment segment = entry.getKey();
        int totalReplicantsInCluster = params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier());

    	double newReplicationFactor = Math.ceil(entry.getValue().doubleValue() * historicalNodeCount / totalWeightCount);
    	
    	if (newReplicationFactor == 0)
    		removeList.put(segment, -1);
    	else if (newReplicationFactor < totalReplicantsInCluster)
    		removeList.put(segment, totalReplicantsInCluster - newReplicationFactor);
    	else if (newReplicationFactor > totalReplicantsInCluster)
    		insertList.put(segment, newReplicationFactor - totalReplicantsInCluster);
    }
  }

  private void manageReplicas(DruidCoordinatorRuntimeParams params, HashMap<DataSegment,Number> insertList, HashMap<DataSegment,Number> removeList, CoordinatorStats stats)
  {
    log.info("Managing Replicas by inserting and removing replicas for relevant data segments");
    
    List<ServerHolder> serverHolderList = new ArrayList<ServerHolder>();
    for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier())
    	serverHolderList.addAll(serverQueue);

    if (serverHolderList.size() == 0) {
      log.makeAlert("Cluster has no servers! Check your cluster configuration!").emit();
      return;
    }
    
    final List<String> tierNameList = Lists.newArrayList(params.getDruidCluster().getTierNames());
    if (tierNameList.size() == 0) {
        log.makeAlert("Cluster has multiple tiers! Check your cluster configuration!").emit();
        return;    	
    }
    final String tier = tierNameList.get(0);

    final DateTime referenceTimestamp = params.getBalancerReferenceTimestamp();
    final BalancerStrategy strategy = params.getBalancerStrategyFactory().createBalancerStrategy(referenceTimestamp);
    
    for (HashMap.Entry<DataSegment,Number> entry : insertList.entrySet())
    {
    	DataSegment segment = entry.getKey();
        CoordinatorStats assignStats = assign(
                params.getReplicationManager(),
                tier,
                strategy,
                serverHolderList,
                segment,
                entry.getValue().intValue()
            );
    	stats.accumulate(assignStats);
    }

    for (HashMap.Entry<DataSegment,Number> entry : removeList.entrySet())
    {
    	DataSegment segment = entry.getKey();
        int totalReplicantsInCluster = params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier());
        if (totalReplicantsInCluster <= 0) {
        	continue;
        }
       
       	int numReplicantsToRemove = entry.getValue().intValue() == -1 ? totalReplicantsInCluster : entry.getValue().intValue();
        CoordinatorStats dropStats = drop(
                params.getReplicationManager(),
                tier,
                serverHolderList,
                segment,
                numReplicantsToRemove
            );
    	stats.accumulate(dropStats);    	
    }
  }

  private CoordinatorStats assign(
      final ReplicationThrottler replicationManager,
      final String tier,
      final BalancerStrategy strategy,
      final List<ServerHolder> serverHolderList,
      final DataSegment segment,
      final int numReplicantsToAdd
  )
  {
    final CoordinatorStats stats = new CoordinatorStats();
    stats.addToTieredStat(assignedCount, tier, 0);

    int numReplicants = numReplicantsToAdd;
    while (numReplicants > 0) {
      final ServerHolder holder = strategy.findNewSegmentHomeReplicator(segment, serverHolderList);

      if (holder == null) {
        log.warn(
            "Not enough [%s] servers or node capacity to assign segment[%s]!",
            tier,
            segment.getIdentifier()
        );
        break;
      }

	  replicationManager.registerReplicantCreation(
	    tier, segment.getIdentifier(), holder.getServer().getHost()
	  );

      holder.getPeon().loadSegment(
          segment,
          new LoadPeonCallback()
          {
            @Override
            public void execute()
            {
              replicationManager.unregisterReplicantCreation(
                  tier,
                  segment.getIdentifier(),
                  holder.getServer().getHost()
              );
            }
          }
      );

      stats.addToTieredStat(assignedCount, tier, 1);
      --numReplicants;
    }

    return stats;
  }

  private CoordinatorStats drop(
	  final ReplicationThrottler replicationManager,
	  final String tier,
      final List<ServerHolder> serverHolderList,
      final DataSegment segment,
      final int numReplicantsToRemove
  )
  {
    CoordinatorStats stats = new CoordinatorStats();
    
    // Pick the server which has the maximum number of segments for load balance
    Map<Number, ServerHolder> segmentCountMap = new TreeMap<Number, ServerHolder>(Collections.reverseOrder());
    for (ServerHolder serverHolder : serverHolderList)
    	if (serverHolder.getServer().getSegment(segment.getIdentifier()) != null)
    		segmentCountMap.put(serverHolder.getServer().getSegments().size(), serverHolder);

    int numReplicants = numReplicantsToRemove;
    for (Map.Entry<Number, ServerHolder> entry : segmentCountMap.entrySet())
    {
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
        if (numReplicants == 0)
        	break;
        
        stats.addToTieredStat(droppedCount, tier, 1);
    }
    return stats;
  }
}
